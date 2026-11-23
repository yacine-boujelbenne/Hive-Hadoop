#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

docker exec hive-server sh -lc '/opt/hive/bin/beeline -u "jdbc:hive2://localhost:10000/default" -n hive -e "SHOW TABLES; SHOW PARTITIONS customers_by_country; SHOW PARTITIONS customers_static; SELECT * FROM customers LIMIT 10; SELECT * FROM jobs LIMIT 5; SELECT * FROM investors;"'

echo "HDFS bucket files:"
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -ls /user/hive/warehouse/customers_bucketed

echo "HDFS ORC file:"
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -ls /user/hive/warehouse/customers_orc
