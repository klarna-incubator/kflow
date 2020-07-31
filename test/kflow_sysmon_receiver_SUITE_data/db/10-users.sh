#!/bin/bash
set -euxo

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    create user kflow with password '$POSTGRES_KFLOW_PWD';
    create user grafana with password '$POSTGRES_GRAFANA_PWD';
EOSQL
