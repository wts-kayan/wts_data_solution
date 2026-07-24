# Cas pratiques Java / Spark — Entretien Data Engineer (domaine bancaire)

> **Format conseillé** : cas 1 en 20–25 min (au tableau ou sur poste, sans exécution obligatoire), cas 2 en 30–35 min (discussion de conception + pseudo-code + questions de suivi).
> L'objectif n'est pas d'obtenir un code qui compile, mais d'observer le raisonnement : choix des types, gestion des nulls, conscience des shuffles, testabilité.

Versions cibles : **Java 8+ / Spark 3.5 / Scala 2.12**.

---

## Cas 1 — Niveau simple : agrégation d'opérations bancaires (DataFrame + Dataset)

### Contexte métier

Une banque de détail dépose chaque jour trois fichiers CSV sur HDFS :

- `clients.csv` — référentiel client
- `comptes.csv` — comptes rattachés aux clients
- `operations.csv` — opérations du jour

On veut produire une table quotidienne du **solde des mouvements par compte**, enrichie du segment client, pour alimenter un reporting.

### Jeux de données

**clients.csv**
```csv
id_client;nom;segment;pays
C001;DUPONT;RETAIL;FR
C002;MARTIN;PRIVATE;FR
C003;NGUYEN;RETAIL;BE
C004;LEROY;CORPORATE;FR
```

**comptes.csv**
```csv
id_compte;id_client;devise;date_ouverture
A100;C001;EUR;2019-03-14
A101;C001;EUR;2021-07-02
A200;C002;EUR;2015-11-30
A300;C003;EUR;2022-01-05
A400;C009;EUR;2020-05-20
```

> Noter `A400` rattaché à un client `C009` **absent du référentiel** : c'est volontaire.

**operations.csv**
```csv
id_operation;id_compte;date_operation;montant;sens;libelle
OP1;A100;2026-07-22;1250.00;C;VIREMENT SALAIRE
OP2;A100;2026-07-22;-45.90;D;CARTE SUPERMARCHE
OP3;A100;2026-07-22;-120.00;D;PRELEVEMENT ENERGIE
OP4;A101;2026-07-22;-15.00;D;FRAIS TENUE COMPTE
OP5;A200;2026-07-22;9800.00;C;VIREMENT EXTERNE
OP6;A200;2026-07-22;-2300.50;D;VIREMENT SORTANT
OP7;A300;2026-07-22;;D;CARTE ESSENCE
OP8;A400;2026-07-22;300.00;C;DEPOT ESPECES
OP9;A100;2026-07-22;-45.90;D;CARTE SUPERMARCHE
```

> Deux pièges volontaires : `OP7` a un **montant null**, `OP9` est un **doublon exact** de `OP2` (hors identifiant).

### Consignes données au candidat

1. Charger les trois fichiers en `Dataset<Row>` avec un **schéma explicite** (pas d'`inferSchema`). Expliquer pourquoi.
2. Écarter les opérations dont le montant est null, **en les traçant** plutôt qu'en les supprimant silencieusement.
3. Dédoublonner les opérations sur la clé métier (`id_compte`, `date_operation`, `montant`, `libelle`).
4. Calculer par compte : nombre d'opérations, total des crédits, total des débits, solde net.
5. Enrichir avec le `segment` et le `pays` du client. Les comptes sans client connu **doivent apparaître** avec un segment `INCONNU`.
6. Convertir le résultat en `Dataset<SoldeCompte>` typé, puis écrire en ORC partitionné par `date_traitement`.
7. Question ouverte : que faudrait-il pour rendre ce job rejouable sans doublonner la sortie ?

### Squelette fourni

```java
public class SoldeCompteJob {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("solde-compte-quotidien")
                .getOrCreate();

        String dateTraitement = args[0];   // ex : 2026-07-22
        String inputPath      = args[1];
        String outputPath     = args[2];

        // TODO
        spark.stop();
    }
}
```

Bean à compléter par le candidat :

```java
public class SoldeCompte implements Serializable {
    private String idCompte;
    private String idClient;
    private String segment;
    private long   nbOperations;
    private double totalCredit;
    private double totalDebit;
    private double soldeNet;
    // getters / setters
}
```

### Solution de référence

```java
StructType schemaOperations = new StructType()
        .add("id_operation",   DataTypes.StringType,  false)
        .add("id_compte",      DataTypes.StringType,  false)
        .add("date_operation", DataTypes.DateType,    false)
        .add("montant",        DataTypes.createDecimalType(18, 2), true)
        .add("sens",           DataTypes.StringType,  true)
        .add("libelle",        DataTypes.StringType,  true);

Dataset<Row> operations = spark.read()
        .option("header", "true")
        .option("delimiter", ";")
        .schema(schemaOperations)
        .csv(inputPath + "/operations.csv");

// 2. Isolation des rejets plutôt que filtrage silencieux
Dataset<Row> rejets = operations.filter(col("montant").isNull())
        .withColumn("motif_rejet", lit("MONTANT_NULL"));
rejets.write().mode(SaveMode.Append)
      .partitionBy("date_operation")
      .orc(outputPath + "/rejets");

Dataset<Row> valides = operations.filter(col("montant").isNotNull());

// 3. Dédoublonnage sur clé métier
Dataset<Row> dedoublonnees = valides.dropDuplicates(
        "id_compte", "date_operation", "montant", "libelle");

// 4. Agrégation
Dataset<Row> agregat = dedoublonnees.groupBy("id_compte").agg(
        count(lit(1)).as("nb_operations"),
        sum(when(col("montant").gt(0), col("montant")).otherwise(lit(0)))
                .cast("double").as("total_credit"),
        sum(when(col("montant").lt(0), col("montant")).otherwise(lit(0)))
                .cast("double").as("total_debit"),
        sum(col("montant")).cast("double").as("solde_net"));

// 5. Enrichissement : comptes puis clients, en left join
Dataset<Row> enrichi = agregat
        .join(comptes.select("id_compte", "id_client"), "id_compte")   // inner assumé : tout compte existe
        .join(broadcast(clients.select("id_client", "segment", "pays")),
              JavaConverters.asScalaBuffer(Arrays.asList("id_client")).toSeq(),
              "left")
        .withColumn("segment", coalesce(col("segment"), lit("INCONNU")))
        .withColumn("pays",    coalesce(col("pays"),    lit("INCONNU")));

// 6. Passage en Dataset typé
Dataset<SoldeCompte> typed = enrichi
        .withColumnRenamed("id_compte", "idCompte")
        .withColumnRenamed("id_client", "idClient")
        .withColumnRenamed("nb_operations", "nbOperations")
        .withColumnRenamed("total_credit",  "totalCredit")
        .withColumnRenamed("total_debit",   "totalDebit")
        .withColumnRenamed("solde_net",     "soldeNet")
        .as(Encoders.bean(SoldeCompte.class));

typed.toDF()
     .withColumn("date_traitement", lit(dateTraitement))
     .write()
     .mode(SaveMode.Overwrite)          // idempotence : voir question 7
     .partitionBy("date_traitement")
     .orc(outputPath + "/solde_compte");
```

### Résultat attendu

| id_compte | id_client | segment | nb_operations | total_credit | total_debit | solde_net |
|---|---|---|---|---|---|---|
| A100 | C001 | RETAIL | 3 | 1250.00 | -165.90 | 1084.10 |
| A101 | C001 | RETAIL | 1 | 0.00 | -15.00 | -15.00 |
| A200 | C002 | PRIVATE | 2 | 9800.00 | -2300.50 | 7499.50 |
| A400 | C009 | INCONNU | 1 | 300.00 | 0.00 | 300.00 |

`A300` n'apparaît pas (unique opération rejetée) — **le candidat doit le signaler spontanément**.

### Grille d'observation — cas 1

| Point | Attendu | Signal faible |
|---|---|---|
| Schéma explicite | Cite la stabilité de production et le coût de l'inférence (scan complet) | Utilise `inferSchema` sans commentaire |
| Type du montant | `DecimalType` ou entier de centimes pour du monétaire | `double` sans aucune réserve sur les arrondis |
| Nulls | Trace les rejets dans une table dédiée | `.na().drop()` en silence |
| Dédoublonnage | Distingue clé technique et clé métier | `distinct()` sur toutes les colonnes, sans voir que l'id diffère |
| Left join | Repère spontanément `C009` et propose `INCONNU` | Fait un inner join et perd la ligne sans le voir |
| Broadcast | Diffuse le référentiel client (petit) | Ne fait aucun choix de stratégie de jointure |
| DataFrame vs Dataset | Sait que `Encoders.bean` exige un JavaBean sérialisable, noms de champs alignés | Croit que `.as()` fait de la conversion de noms automatique |
| Idempotence | `Overwrite` de la partition ciblée, ou suppression préalable | Écrit en `Append` sans y penser |

### Questions de suivi

- Pourquoi `Dataset<Row>` est-il souvent préférable à `Dataset<SoldeCompte>` en Java ? *(coût des encodeurs bean, Catalyst ne voit pas dans les lambdas typées, verbosité des getters/setters)*
- Combien de shuffles dans ce job ? *(le `groupBy`, plus la jointure sur `comptes` si elle n'est pas broadcastée)*
- Si `operations.csv` faisait 400 Go, qu'est-ce qui casserait en premier ?
- Comment testeriez-vous l'agrégation sans HDFS ?

---

## Cas 2 — Niveau moyen : impayés, bucketisation d'ancienneté et exposition par segment

### Contexte métier

Sur un périmètre de crédits, on reçoit chaque mois un **snapshot** des contrats et un flux d'**échéances impayées**. On doit produire une table mensuelle d'**exposition par segment et par tranche d'ancienneté d'impayé** (le fameux « aging » ou bucket DPD — *days past due*), avec pour chaque contrat le nombre de jours de retard le plus ancien.

Règles métier :

- Le DPD d'un contrat = nombre de jours entre la **date d'échéance impayée la plus ancienne non régularisée** et la date d'arrêté.
- Buckets : `SAIN` (0), `B1_30`, `B31_60`, `B61_90`, `B90_PLUS`.
- Un contrat en `B90_PLUS` est marqué `en_defaut = true`.
- Le snapshot contrats peut contenir **plusieurs lignes par contrat** (corrections successives) : ne retenir que la plus récente par `date_maj`.
- Le taux de change vers l'euro provient d'un petit référentiel devise/mois.
- Sortie attendue : par (`date_arrete`, `segment`, `bucket`) → nombre de contrats, exposition totale en EUR, exposition en défaut.

### Schémas d'entrée

```
contrats           (id_contrat, id_client, segment, devise, encours, date_maj, date_arrete)
echeances_impayees (id_contrat, date_echeance, montant_du, statut)   -- statut : IMPAYE / REGULARISE
taux_change        (devise, mois, taux_eur)
```

Volumétrie annoncée au candidat : **contrats ≈ 80 millions de lignes**, **échéances ≈ 900 millions**, **taux_change ≈ 300 lignes**. La distribution des contrats par segment est très déséquilibrée : le segment `RETAIL` représente 85 % du volume.

### Consignes

1. Déduire un snapshot unique par contrat (dernière `date_maj`, départage déterministe en cas d'égalité).
2. Calculer le DPD par contrat à partir des échéances au statut `IMPAYE`.
3. Attribuer le bucket et le drapeau `en_defaut`.
4. Convertir l'encours en EUR.
5. Agréger par (`date_arrete`, `segment`, `bucket`).
6. Écrire en ORC partitionné par `date_arrete`, en visant des fichiers de taille raisonnable.
7. Décrire les optimisations retenues et **pourquoi** (le candidat doit justifier, pas réciter).

### Solution de référence (extraits)

```java
// 1. Déduplication du snapshot : une ligne par contrat
WindowSpec wContrat = Window.partitionBy("id_contrat")
                            .orderBy(col("date_maj").desc(), col("encours").desc());

Dataset<Row> contratsUniques = contrats
        .withColumn("rn", row_number().over(wContrat))
        .filter(col("rn").equalTo(1))
        .drop("rn");

// 2. DPD : plus ancienne échéance impayée par contrat
Dataset<Row> dpd = echeances
        .filter(col("statut").equalTo("IMPAYE"))
        .groupBy("id_contrat")
        .agg(min("date_echeance").as("date_impaye_min"),
             sum("montant_du").as("montant_impaye"));

// 3. Jointure + calcul du bucket
Dataset<Row> base = contratsUniques
        .join(dpd, JavaConverters.asScalaBuffer(Arrays.asList("id_contrat")).toSeq(), "left")
        .withColumn("dpd",
              when(col("date_impaye_min").isNull(), lit(0))
             .otherwise(datediff(col("date_arrete"), col("date_impaye_min"))))
        .withColumn("bucket",
              when(col("dpd").leq(0),  lit("SAIN"))
             .when(col("dpd").leq(30), lit("B1_30"))
             .when(col("dpd").leq(60), lit("B31_60"))
             .when(col("dpd").leq(90), lit("B61_90"))
             .otherwise(lit("B90_PLUS")))
        .withColumn("en_defaut", col("dpd").gt(90));

// 4. Conversion devise — petit référentiel : broadcast explicite
Dataset<Row> avecTaux = base
        .withColumn("mois", date_format(col("date_arrete"), "yyyy-MM"))
        .join(broadcast(taux),
              JavaConverters.asScalaBuffer(Arrays.asList("devise", "mois")).toSeq(),
              "left")
        .withColumn("encours_eur", col("encours").multiply(coalesce(col("taux_eur"), lit(1.0))));

// 5. Agrégation finale
Dataset<Row> resultat = avecTaux
        .groupBy("date_arrete", "segment", "bucket")
        .agg(count(lit(1)).as("nb_contrats"),
             sum("encours_eur").as("exposition_eur"),
             sum(when(col("en_defaut"), col("encours_eur")).otherwise(lit(0))).as("exposition_defaut"));

// 6. Écriture maîtrisée
resultat.repartition(col("date_arrete"))
        .write().mode(SaveMode.Overwrite)
        .partitionBy("date_arrete")
        .orc(outputPath + "/aging_impayes");
```

### Ce que le cas doit faire émerger

**a) La pré-agrégation avant jointure.**
Joindre 900 M d'échéances à 80 M de contrats puis agréger serait bien plus coûteux que d'agréger d'abord les échéances (900 M → ~quelques millions de contrats en impayé) et de joindre ensuite. Un bon candidat le propose spontanément.

**b) Le choix de la stratégie de jointure.**
`taux_change` (300 lignes) doit être broadcasté — explicitement, car après agrégations les statistiques peuvent être mauvaises. La jointure contrats/DPD est un vrai shuffle, sur une clé bien distribuée (`id_contrat`) donc peu risquée.

**c) Le skew.**
Le déséquilibre annoncé est sur `segment`, donc il ne touche **que le `groupBy` final** — qui produit très peu de lignes. Le candidat qui affirme « il faut salter la jointure à cause de RETAIL » n'a pas vu que la jointure se fait sur `id_contrat`. C'est le meilleur discriminant du cas : sait-il localiser un skew au lieu d'appliquer un réflexe ?

**d) Window function vs auto-jointure.**
`row_number()` sur `date_maj` est la réponse attendue. Attention au départage des ex-aequo : sans second critère de tri, le résultat n'est pas déterministe d'un run à l'autre — critique en contexte réglementaire.

**e) La gestion des cas limites.**
- Contrat sans échéance impayée → `left join`, DPD = 0, bucket `SAIN` (pas de perte de ligne).
- Taux de change manquant → `coalesce` à 1.0 est **discutable** : un bon candidat propose plutôt de rejeter la ligne ou d'alerter, parce qu'un taux implicite à 1 fausse silencieusement un montant réglementaire.
- Échéance postérieure à la date d'arrêté → DPD négatif, à borner à 0.
- Bornes des buckets : `<= 30` ou `< 30` ? Faire expliciter la convention plutôt que la deviner.

**f) La sortie.**
Le résultat final est minuscule (quelques centaines de lignes) : `repartition` avant écriture évite des milliers de fichiers vides issus du parallélisme du shuffle.

### Grille d'observation — cas 2

| Critère | Excellent | Correct | Insuffisant |
|---|---|---|---|
| Ordre des opérations | Pré-agrège les échéances avant de joindre | Joint puis agrège mais le voit quand on l'interroge | Ne voit pas le problème même après relance |
| Stratégie de jointure | Broadcast justifié, connaît le seuil et ses limites | Cite le broadcast sans justification | Ne mentionne aucune stratégie |
| Skew | Localise correctement le déséquilibre sur le `groupBy` | Parle de skew de façon générale | Applique du salting au mauvais endroit |
| Déterminisme | Départage explicite des ex-aequo, pense à l'auditabilité | Utilise `row_number` sans second critère | Utilise `dropDuplicates` sur un snapshot ordonné |
| Cas limites | Remonte spontanément taux manquant / DPD négatif | Les traite quand on les soulève | Ne les considère pas |
| Sortie | Maîtrise le nombre de fichiers | Écrit sans y penser | `coalesce(1)` sur un gros volume |
| Testabilité | Isole les règles de bucketisation dans une fonction testable | Code monolithique mais lisible | Tout dans le `main` |

### Questions de suivi

1. On vous demande d'ajouter un historique : conserver 36 arrêtés mensuels dans la même table. Quel partitionnement et quel impact sur les lectures ?
2. Le job tourne en 2 h, dont 1 h 20 sur une seule étape. Comment identifiez-vous laquelle, dans la Spark UI ?
3. Le contrôleur constate un écart de 3 contrats entre le snapshot et la sortie. Comment le tracez-vous ?
4. Comment feriez-vous un test de non-régression entre deux versions du code sur un même jeu d'entrée ?
5. Si la règle de bucket devient paramétrable par entité juridique, comment structurez-vous le code ?

---

## Barème indicatif

| Dimension | Poids |
|---|---|
| Justesse fonctionnelle (le résultat est bon, cas limites compris) | 30 % |
| Maîtrise Spark (shuffles, jointures, partitionnement, plan) | 30 % |
| Qualité du code Java (structure, testabilité, typage, nommage) | 20 % |
| Rigueur données (nulls, déterminisme, traçabilité, idempotence) | 20 % |

**Seuil recommandé** : un profil confirmé doit traiter le cas 1 sans aide et proposer au moins la pré-agrégation et le broadcast sur le cas 2. Un profil senior doit repérer seul le piège du skew et discuter le traitement du taux de change manquant.

---

## Notes pour l'examinateur

- Annoncer d'emblée que la syntaxe exacte n'est pas notée et qu'il peut demander des rappels d'API. Ce qu'on évalue, c'est la démarche.
- Laisser les pièges (le doublon, le client absent, le montant null) se révéler d'eux-mêmes. Ne pas les pointer avant 10 minutes.
- Si le candidat bloque sur le cas 2, donner un indice par palier : *« combien pèsent les échéances par rapport aux contrats ? »*, puis *« et si on agrégeait avant de joindre ? »*. La vitesse de rebond après indice est en soi une information.
- Garder 5 minutes pour lui demander ce qu'il aurait fait différemment avec plus de temps. Les meilleurs candidats citent les tests et le monitoring.
