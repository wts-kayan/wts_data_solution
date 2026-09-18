package com.bnpp.itg.fresh.str.utils;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Builds an {@link Encoder} for {@link Row} on any Spark version from 3.0 to 3.5.
 *
 * <p>The factory for a Row encoder moved in Spark 3.5:
 * <ul>
 *   <li>Spark &le; 3.4: {@code org.apache.spark.sql.catalyst.encoders.RowEncoder.apply(StructType)}
 *       returning {@code ExpressionEncoder&lt;Row&gt;}. Lives in {@code spark-catalyst}.</li>
 *   <li>Spark &ge; 3.5: {@code org.apache.spark.sql.Encoders.row(StructType)} returning
 *       {@code Encoder&lt;Row&gt;}. {@code RowEncoder} still exists but only exposes
 *       {@code encoderFor}, and it moved to {@code spark-sql-api}.</li>
 * </ul>
 *
 * <p>Both are resolved reflectively, so the compiled jar links against neither and runs on
 * both. Calling either one directly is what produces
 * {@code NoSuchMethodError: org.apache.spark.sql.Encoders.row} on the version that does not
 * have it.
 *
 * <p>The lookup runs once and the result is cached: reflection costs nothing per call, and the
 * failure (if the running Spark has neither factory) surfaces at class-init with a message
 * naming the Spark version rather than as a {@code NoSuchMethodError} deep in a step.
 */
public final class RowEncoderCompat {

    private static final Method FACTORY;
    private static final String FACTORY_NAME;

    static {
        Method factory = null;
        String name = null;

        // Spark >= 3.5
        try {
            factory = Encoders.class.getMethod("row", StructType.class);
            name = "org.apache.spark.sql.Encoders.row";
        } catch (NoSuchMethodException notSpark35) {
            // Spark <= 3.4
            try {
                Class<?> rowEncoder = Class.forName("org.apache.spark.sql.catalyst.encoders.RowEncoder");
                factory = rowEncoder.getMethod("apply", StructType.class);
                name = "org.apache.spark.sql.catalyst.encoders.RowEncoder.apply";
            } catch (ClassNotFoundException | NoSuchMethodException notSpark34) {
                throw new IllegalStateException(
                        "No Row encoder factory found on the classpath. Neither Encoders.row(StructType) "
                                + "(Spark >= 3.5) nor RowEncoder.apply(StructType) (Spark <= 3.4) is available. "
                                + "Spark version: " + org.apache.spark.package$.MODULE$.SPARK_VERSION(),
                        notSpark34);
            }
        }

        FACTORY = factory;
        FACTORY_NAME = name;
    }

    private RowEncoderCompat() {
    }

    /**
     * @param schema the schema of the rows to encode
     * @return a Row encoder built by whichever factory the running Spark provides
     */
    @SuppressWarnings("unchecked")
    public static Encoder<Row> of(StructType schema) {
        try {
            return (Encoder<Row>) FACTORY.invoke(null, schema);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot call " + FACTORY_NAME, e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalStateException(FACTORY_NAME + " failed for schema " + schema.catalogString(), cause);
        }
    }

    /** The factory actually resolved on this cluster. Log it once at startup if you want it in the run log. */
    public static String resolvedFactory() {
        return FACTORY_NAME;
    }
}
