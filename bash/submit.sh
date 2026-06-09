#!/bin/bash

# Uncomment and set these variables if needed
    PYMAIN=${1}
# Print the PATH environment variable and locate spark-submit
echo $PATH

# Function to handle Kerberos authentication
function authentication() {
    USER=$(whoami)
    echo "Utilisateur : ${USER}"
    echo "kinit -kt /etc/security/keytabs/${USER}.keytab ${USER}"
    kinit -kt "/etc/security/keytabs/${USER}.keytab" "${USER}"
    echo "klist"
    klist
}

# Main function
function main() {
    authentication
    # Base database directory
    DB_DIR="/Projects/STCreditRisk_UAT/hive/databases/dbprojection.db"

    # Loop over all sub-directories (tables) under the database
    for table_dir in $(hdfs dfs -ls "$DB_DIR" | grep '^d' | awk '{print $8}'); do
        echo "=== Processing directory: $table_dir ==="

        # Get all partitions with runid= under this directory
        for partition in $(hdfs dfs -ls "$table_dir" | grep 'runid=' | awk '{print $8}'); do
            # Create the new path with runId=
            new_path=$(echo "$partition" | sed 's/runid=/runId=/')

            echo "Renaming $partition to $new_path"
            hdfs dfs -mv "$partition" "$new_path"
        done
    done
    echo "Partition renaming complete"
}

# Redirect stderr to stdout and call the main function
main 2>&1