-- Section 2 - Hive fundamentals and storage optimization

CREATE DATABASE IF NOT EXISTS section;
DROP TABLE IF EXISTS test_connection;

-- 2.1 Row format and TSV storage
CREATE TABLE IF NOT EXISTS jobs (
  id INT,
  title STRING,
  salary INT,
  posted TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE jobs
VALUES
  (1, 'Data Analyst', 135000, '2016-12-21 15:52:03'),
  (2, 'Software Engineer', 150000, '2026-11-22 18:00:00');

CREATE TABLE IF NOT EXISTS jobs_tsv (
  id INT,
  title STRING,
  salary INT,
  posted TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE jobs_tsv
VALUES
  (1, 'Data Analyst', 135000, '2016-12-21 15:52:03'),
  (2, 'Software Engineer', 150000, '2026-11-22 18:00:00');

-- 2.2 Managed and external tables
DROP TABLE IF EXISTS investors;
CREATE EXTERNAL TABLE investors (
  name STRING,
  amount INT,
  share DECIMAL(4,3)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

INSERT INTO investors
VALUES ('Karim Beguir', 10000, 0.325);

-- 2.3 Text and Parquet formats
DROP TABLE IF EXISTS jobs_txt;
CREATE TABLE jobs_txt
STORED AS TEXTFILE
AS SELECT * FROM jobs;

DROP TABLE IF EXISTS jobs_parquet;
CREATE TABLE jobs_parquet
STORED AS PARQUET
AS SELECT * FROM jobs;

-- 2.4 Custom HDFS location
DROP TABLE IF EXISTS jobs_training;
CREATE TABLE jobs_training (
  id INT,
  title STRING,
  salary INT,
  posted TIMESTAMP
)
LOCATION 'hdfs://namenode:9000/user/training/jobs_training/';

-- 2.5 Schema cloning and data copy
DROP TABLE IF EXISTS new_jobs;
CREATE TABLE new_jobs LIKE jobs;
INSERT OVERWRITE TABLE new_jobs SELECT * FROM jobs;

-- 2.6 Metadata and renaming
ALTER TABLE new_jobs SET TBLPROPERTIES ('description'='Clone of jobs table');
DROP TABLE IF EXISTS section.companies;
ALTER TABLE new_jobs RENAME TO section.companies;

-- 2.7 Staging customers CSV
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
  id_cust INT,
  name STRING,
  country STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA INPATH 'hdfs://namenode:9000/user/hive/warehouse/customers.csv'
INTO TABLE customers;

-- Static partitioning
DROP TABLE IF EXISTS customers_static;
CREATE TABLE customers_static (
  id_cust INT,
  name STRING
)
PARTITIONED BY (country STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE customers_static
PARTITION (country='France')
SELECT id_cust, name
FROM customers
WHERE country='France';

-- Dynamic partitioning
SET hive.exec.dynamic.partition.mode=nonstrict;
DROP TABLE IF EXISTS customers_by_country;
CREATE TABLE customers_by_country (
  id_cust INT,
  name STRING
)
PARTITIONED BY (country STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE customers_by_country
PARTITION (country)
SELECT id_cust, name, country
FROM customers
WHERE country <> 'country';

-- 2.8 Bucketing
SET hive.enforce.bucketing=true;
DROP TABLE IF EXISTS customers_bucketed;
CREATE TABLE customers_bucketed (
  id_cust INT,
  name STRING,
  country STRING
)
CLUSTERED BY (id_cust)
INTO 8 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE customers_bucketed
SELECT * FROM customers
WHERE country <> 'country';

-- 2.9 ORC, CBO and predicate pushdown
DROP TABLE IF EXISTS customers_orc;
CREATE TABLE customers_orc
STORED AS ORC
AS SELECT * FROM customers
WHERE country <> 'country';

ANALYZE TABLE customers_orc
COMPUTE STATISTICS FOR COLUMNS;

SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.optimize.ppd=true;
SET hive.optimize.ppd.storage=true;
SET hive.orc.splits.include.file.footer=true;
SET hive.optimize.index.filter=true;

-- Verification commands
SHOW TABLES;
SHOW PARTITIONS customers_by_country;
SHOW PARTITIONS customers_static;
DESCRIBE FORMATTED jobs_txt;
DESCRIBE FORMATTED jobs_parquet;
DESCRIBE FORMATTED customers_orc;
EXPLAIN SELECT * FROM customers_orc WHERE country='France';
