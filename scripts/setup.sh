#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

echo "Starting Hive Hadoop TP2 stack..."
docker compose up -d

echo "Waiting for Hive metastore and HiveServer2 containers..."
for i in $(seq 1 60); do
  METASTORE_HEALTH=$(docker inspect --format '{{.State.Health.Status}}' hive-metastore 2>/dev/null || echo starting)
  HIVESERVER_RUNNING=$(docker inspect --format '{{.State.Status}}' hive-server 2>/dev/null || echo starting)
  if [ "$METASTORE_HEALTH" = "healthy" ] && [ "$HIVESERVER_RUNNING" = "running" ]; then
    break
  fi
  sleep 2
done

docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfsadmin -safemode leave >/dev/null 2>&1 || true
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p /user/hive/warehouse /user/training/jobs_training /apps/hive/install
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -chown -R hive:hive /user/hive /user/training /apps/hive
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -chmod -R 775 /user/hive/warehouse /user/training/jobs_training

docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -rm -f /user/hive/warehouse/customers.csv >/dev/null 2>&1 || true
for path in \
  /user/hive/warehouse/investors \
  /user/hive/warehouse/jobs \
  /user/hive/warehouse/jobs_tsv \
  /user/hive/warehouse/jobs_txt \
  /user/hive/warehouse/jobs_parquet \
  /user/hive/warehouse/jobs_training \
  /user/hive/warehouse/customers \
  /user/hive/warehouse/customers_static \
  /user/hive/warehouse/customers_by_country \
  /user/hive/warehouse/customers_bucketed \
  /user/hive/warehouse/customers_orc \
  /user/hive/warehouse/new_jobs \
  /user/hive/warehouse/test_connection \
  /user/hive/warehouse/section.db/companies; do
  docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -rm -r -f "$path" >/dev/null 2>&1 || true
done
docker exec -i namenode /opt/hadoop-3.2.1/bin/hdfs dfs -put -f /dev/stdin /user/hive/warehouse/customers.csv < data_to_import/customers.csv

echo "Executing Section 2 SQL..."
docker exec -i hive-server sh -lc '/opt/hive/bin/beeline -u "jdbc:hive2://localhost:10000/default" -n hive -f /dev/stdin' < sql/section2.sql

echo "Section 2 setup complete."
