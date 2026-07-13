#!/bin/sh
# Bootstraps a streaming replica from postgres-primary on first start
# (pg_basebackup + standby.signal), then runs postgres in standby mode.
# Phase 0 only requires the primary; the replica exists so later phases can
# exercise read scaling and failover.
set -eu

PGDATA="${PGDATA:-/var/lib/postgresql/data}"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
	echo "replica: cloning primary via pg_basebackup"
	until PGPASSWORD="$REPLICATION_PASSWORD" pg_basebackup \
		--pgdata="$PGDATA" \
		--host=postgres-primary \
		--username=replicator \
		--wal-method=stream \
		--checkpoint=fast \
		--write-recovery-conf; do
		echo "replica: primary not ready, retrying in 2s"
		rm -rf "${PGDATA:?}"/*
		sleep 2
	done
	chmod 700 "$PGDATA"
fi

exec postgres
