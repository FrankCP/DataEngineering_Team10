#!/usr/bin/env bash
set -e

echo "Waiting for Postgres..."
while ! nc -z postgres-airflow 5432; do
  sleep 1
done
echo "Postgres is available."

echo "Initializing Airflow database..."
airflow db migrate

echo "Creating admin user (if not exists)..."

set +e
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
EXIT_CODE=$?
set -e

if [ "$EXIT_CODE" -ne 0 ]; then
  echo "Admin user already exists — skipping creation."
else
  echo "Admin user created."
fi

echo "Unpausing all DAGs..."
airflow dags unpause $(airflow dags list --output plain | tail -n +2 | awk '{print $1}') || true

echo "Starting Airflow: airflow $@"
exec airflow "$@"
