#!/bin/sh
# Runs once at first initdb on the primary: creates the streaming-replication
# role and allows replication connections from the compose network.
set -eu

psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<-SQL
	CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '${REPLICATION_PASSWORD}';
SQL

echo "host replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"
