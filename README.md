# Hive–Hadoop TP2 Data Lakehouse

A reproducible Apache Hadoop, Hive, PostgreSQL and Spark environment implementing the TP2 laboratory from Docker setup through Hive fundamentals and storage optimization.

## Current validated state

- Hive 4.0.1 connected through HiveServer2 and Beeline
- Hive Metastore running with PostgreSQL 14
- HDFS warehouse available at `hdfs://namenode:9000/user/hive/warehouse`
- Hive execution engine: MapReduce (`hive.execution.engine=mr`) for a stable local environment
- HDFS warehouse permissions allow writes by the `hive` user
- Section 1 completed
- Section 2 completed
- Section 3 prerequisites documented; source CSV files are intentionally not included

## Architecture

```text
Beeline / Hive JDBC
        |
        v
HiveServer2 :10000
        |
        v
Hive Metastore Thrift :9083
        |
        v
PostgreSQL metastore database
        |
        v
HDFS NameNode :9000
        |
  DataNodes
        |
        v
MapReduce / YARN execution
```

## Repository layout

```text
.
├── docker-compose.yml          Docker stack definition
├── hadoop.env                  Hadoop environment variables
├── hadoop-conf/                Hadoop and YARN XML configuration
├── hive-conf/hive-site.xml     Hive Metastore and warehouse settings
├── spark-conf/                 Spark configuration
├── postgres-jdbc.jar           PostgreSQL JDBC driver used by Hive
├── data_to_import/             Source CSV files
├── sql/
│   ├── section1.sql            Section 1 validation queries
│   └── section2.sql            Complete Section 2 Hive setup
├── scripts/
│   ├── setup.sh                Initialize HDFS and Section 2 data
│   └── verify-section2.sh      Verify Section 2 results
└── README.md
```

## Prerequisites

- Docker Engine with Compose v2
- Linux host with ports 9870, 9864, 8088, 10000, 10002, 9083, 5433 and 5434 available
- At least 8 GB RAM recommended

## Start the platform

```bash
cd /path/to/Hive-Hadoop
docker compose down
docker compose up -d
```

Wait until the containers are healthy:

```bash
docker compose ps
docker inspect --format '{{.State.Status}} {{.State.Health.Status}}' hive-metastore
```

Expected core services:

- `namenode`
- `datanode`
- `resourcemanager`
- `nodemanager`
- `hive-metastore-postgresql`
- `hive-metastore`
- `hive-server`
- `postgres-db`

Leave HDFS safe mode if it is active:

```bash
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfsadmin -safemode leave
```

## Connect to Hive

```bash
docker exec -it hive-server /opt/hive/bin/beeline \
  -u "jdbc:hive2://localhost:10000/default" \
  -n hive
```

Every HQL statement must end with `;` and SQL keywords must retain normal spaces.

Useful commands:

```sql
SHOW DATABASES;
SHOW TABLES;
DESCRIBE FORMATTED jobs;
quit;
```

## Initialize the project automatically

From the host project directory:

```bash
chmod +x scripts/setup.sh scripts/verify-section2.sh
./scripts/setup.sh
./scripts/verify-section2.sh
```

The setup script:

1. leaves HDFS safe mode
2. creates and permissions `/user/hive/warehouse` and `/user/training`
3. uploads `data_to_import/customers.csv` to HDFS
4. executes [sql/section2.sql](sql/section2.sql) through Beeline

## Section 1: environment concepts

### HDFS

HDFS stores data across distributed DataNodes. The NameNode owns the namespace and metadata.

- NameNode web UI: http://localhost:9870
- HDFS address: `hdfs://namenode:9000`
- Warehouse: `/user/hive/warehouse`

Useful HDFS commands:

```bash
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -ls /
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -ls /user/hive/warehouse
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -cat /user/hive/warehouse/jobs_tsv/*
```

### YARN

YARN schedules MapReduce, Hive, Spark and Tez applications.

- ResourceManager UI: http://localhost:8088
- NodeManager UI: http://localhost:8042

### Hive metastore

Hive separates the logical catalogue from physical files:

```text
Beeline → HiveServer2 → Metastore → PostgreSQL
                           |
                           v
                         HDFS
```

PostgreSQL stores Hive catalogue metadata. HDFS stores table data files.

## Section 2: Hive fundamentals

### 1. Row format

`ROW FORMAT DELIMITED` identifies how Hive maps text fields to columns.

```sql
CREATE TABLE jobs_tsv (...)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';
```

### 2. Managed and external tables

Managed tables are controlled by Hive, including their physical lifecycle. External tables keep physical data when the Hive table is dropped.

```sql
CREATE EXTERNAL TABLE investors (
  name STRING,
  amount INT,
  share DECIMAL(4,3)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
```

### 3. Storage formats

- `TEXTFILE`: readable plain text
- `TSV`: text with tab separators
- `PARQUET`: compressed binary columnar format
- `ORC`: compressed columnar format with stripes, indexes and metadata

### 4. Custom locations

`LOCATION` controls the HDFS directory used by a table:

```sql
CREATE TABLE jobs_training (...)
LOCATION 'hdfs://namenode:9000/user/training/jobs_training/';
```

### 5. Cloning

`LIKE` copies the schema, not the rows:

```sql
CREATE TABLE new_jobs LIKE jobs;
INSERT INTO new_jobs SELECT * FROM jobs;
```

### 6. Metadata and alteration

```sql
DESCRIBE jobs;
DESCRIBE FORMATTED jobs_txt;
ALTER TABLE new_jobs RENAME TO section.companies;
```

### 7. Partitioning

Partitioning creates separate HDFS directories by partition value.

Static partitioning supplies the value explicitly:

```sql
INSERT OVERWRITE TABLE customers_static
PARTITION (country='France')
SELECT id_cust, name
FROM customers
WHERE country='France';
```

Dynamic partitioning obtains values from a SELECT:

```sql
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE customers_by_country
PARTITION (country)
SELECT id_cust, name, country
FROM customers;
```

Resulting directories look like:

```text
country=France/
country=Spain/
country=Tunisia/
country=USA/
```

Partition pruning skips unrelated directories.

### 8. Bucketing

Bucketing writes a fixed number of files based on a hash of the bucket column:

```sql
CREATE TABLE customers_bucketed (...)
CLUSTERED BY (id_cust)
INTO 8 BUCKETS;
```

The validated table contains exactly 8 HDFS data files. Bucketing supports efficient sampling and, for compatible bucketed joins, map-side joins.

### 9. ORC, CBO and predicate pushdown

ORC stores column data in stripes and includes metadata used to skip blocks.

```sql
CREATE TABLE customers_orc
STORED AS ORC
AS SELECT * FROM customers;

ANALYZE TABLE customers_orc
COMPUTE STATISTICS FOR COLUMNS;

SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.optimize.ppd=true;
SET hive.optimize.ppd.storage=true;
SET hive.orc.splits.include.file.footer=true;
SET hive.optimize.index.filter=true;

EXPLAIN SELECT * FROM customers_orc
WHERE country='France';
```

CBO uses statistics to choose a query plan. Predicate Pushdown sends filters to the storage reader so it can skip irrelevant ORC stripes.

## Section 3 overview

Section 3 imports relational PostgreSQL data into Hive using Sqoop:

```text
PostgreSQL temp_db
        |
        v
Sqoop
        |
        v
YARN / MapReduce
        |
        v
HDFS
        |
        v
Hive Metastore
        |
        v
lab2_big_data tables
```

To continue, download `order_detail.csv` and `restaurant_detail.csv`, place them in `data_to_import/`, then follow the SQL and Sqoop commands in the TP instructions.

## Public endpoints

| Service | URL |
|---|---|
| NameNode | http://localhost:9870 |
| DataNode | http://localhost:9864 |
| ResourceManager | http://localhost:8088 |
| HistoryServer | http://localhost:8188 |
| HiveServer2 JDBC | localhost:10000 |
| Hive Metastore | localhost:9083 |
| Application PostgreSQL | localhost:5433 |
| Metastore PostgreSQL | localhost:5434 |

## Troubleshooting

### HiveServer2 reports no connection

Confirm HiveServer2 is listening:

```bash
docker compose ps hive-server
docker logs --tail 100 hive-server
```

Reconnect with the JDBC URI used above.

### HDFS write permission denied

```bash
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfsadmin -safemode leave
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -chown -R hive:hive /user/hive
docker exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -chmod -R 775 /user/hive/warehouse
```

### Hive Metastore unhealthy

```bash
docker logs --tail 300 hive-metastore
```

The PostgreSQL JDBC JAR must exist at `postgres-jdbc.jar` and contain `org/postgresql/Driver.class`.

### Reset the complete environment

This deletes HDFS and PostgreSQL named volumes:

```bash
docker compose down -v
rm -rf pgdata
docker compose up -d
./scripts/setup.sh
```

## Key database credentials

Educational development credentials:

| Database | Host | Database | User | Password |
|---|---|---|---|---|
| Application | localhost:5433 | temp_db | postgres | passw0rd |
| Metastore | localhost:5434 | metastore | hive | hive_password |

These credentials are for a local TP environment and should not be reused in production.
