#!/bin/bash
# Exécuté une seule fois, au premier démarrage (volume Postgres vide).
# Crée deux bases séparées dans la même instance :
#   - AIRFLOW_DB_NAME : métadonnées d'Airflow
#   - VELIB_DB_NAME   : données du projet (schémas raw / staging / core / marts)
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname postgres \
  -v airflow_db="$AIRFLOW_DB_NAME" \
  -v airflow_user="$AIRFLOW_DB_USER" \
  -v airflow_pwd="$AIRFLOW_DB_PASSWORD" \
  -v velib_db="$VELIB_DB_NAME" \
  -v velib_user="$VELIB_DB_USER" \
  -v velib_pwd="$VELIB_DB_PASSWORD" <<-'EOSQL'
	CREATE ROLE :"airflow_user" WITH LOGIN PASSWORD :'airflow_pwd';
	CREATE DATABASE :"airflow_db" OWNER :"airflow_user";

	CREATE ROLE :"velib_user" WITH LOGIN PASSWORD :'velib_pwd';
	CREATE DATABASE :"velib_db" OWNER :"velib_user";
EOSQL
