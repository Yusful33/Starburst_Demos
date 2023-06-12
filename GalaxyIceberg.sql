--Step 1 (Creating Schema)
Create schema if not exists demo_aws_s3.trino_iceberg_edu 
    with (location = 's3://yusuf-cattaneo-bootcamp-nov2022/trino_iceberg_edu/');

USE demo_aws_s3.trino_iceberg_edu;

DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS orders;
DROP SCHEMA IF EXISTS iceberg;

--Creating Tables 

CREATE TABLE
  orders WITH (type = 'HIVE', format = 'ORC') AS
SELECT
  orderkey,
  custkey,
  orderstatus,
  totalprice,
  orderpriority,
  clerk
FROM tpch.tiny.orders;

alter table orders SET PROPERTIES type = 'ICEBERG';

SELECT * FROM "orders$properties";

CREATE TABLE customer
WITH (
  partitioning = ARRAY['mktsegment'],
  sorted_by = ARRAY['custkey'],
  format='parquet',
  type = 'ICEBERG'
) AS
SELECT
    c.custkey,
    c.name,
    c.mktsegment,
    ROUND(c.acctbal) as account_balance,
    n.name as nation,
    r.name as region
FROM
    demo_postgres_aws.demo.customer c 
    join demo_redshift.demo.nation n on c.nationkey = n.nationkey
    join demo_snowflake.demo.region r on r.regionkey = n.regionkey limit 10000;

SELECT * FROM customer;


--------- Metadata Columns & Tables -----------------------------------------------------------------------------

SELECT custkey, "$path", "$file_modified_time" FROM customer;

SELECT * FROM "customer$snapshots";
SELECT * FROM "customer$history";
SELECT * FROM "customer$manifests";
SELECT * FROM "customer$partitions";
SELECT * FROM "customer$files";

------------- INSERT / UPDATE / DELETE / MERGE  ------------------------------------------------------------------

SELECT * FROM customer ORDER BY name;

INSERT INTO customer (custkey, name, mktsegment, account_balance, nation, region)
VALUES (200000, 'COMMANDER BUN BUN', 'SQLENGINE', 1, 'UNITED STATES', 'AMERICA');

SELECT * FROM customer ORDER BY name;
SELECT * FROM "customer$snapshots" ORDER BY committed_at DESC;

UPDATE customer SET account_balance = 1000 WHERE custkey = 200000;
SELECT * FROM customer ORDER BY name;
SELECT * FROM "customer$snapshots" ORDER BY committed_at DESC;

DELETE FROM customer WHERE custkey = 200000;
SELECT * FROM customer ORDER BY name;
SELECT * FROM "customer$snapshots" ORDER BY committed_at DESC;


-------MERGE OPERATION (single command to provide update else insert operations)
drop table if exists customer_raw;

CREATE TABLE if not exists customer_raw
WITH (
  partitioning = ARRAY['mktsegment'],
  sorted_by = ARRAY['custkey'],
  format='parquet'
) AS
SELECT
    c.custkey,
    case when SUBSTRING(c.name, LENGTH(c.name)) in ('1',  '3', '5', '7', '9') THEN SUBSTRING(c.name, 1, LENGTH(c.name) - 1) else c.name end as name,
    c.mktsegment,
    ROUND(c.acctbal) as account_balance,
    n.name as nation,
    r.name as region
FROM
    demo_postgres_aws.demo.customer c 
    join demo_redshift.demo.nation n on c.nationkey = n.nationkey
    join demo_snowflake.demo.region r on r.regionkey = n.regionkey limit 10000;


SELECT count(*) FROM customer;

MERGE INTO customer AS b
USING customer_raw AS l
ON (b.custkey = l.custkey)
WHEN MATCHED and b.name != l.name
THEN UPDATE
SET name = l.name ,
    mktsegment = l.mktsegment,
    account_balance = l.account_balance,
    nation = l.nation,
    region = l.region
WHEN NOT MATCHED
      THEN INSERT (custkey, name, mktsegment, account_balance, nation, region)
            VALUES(l.custkey, l.name, l.mktsegment,  l.account_balance, l.nation, l.region);

SELECT * FROM "customer$snapshots" ORDER BY committed_at DESC;

SELECT length(name) as name_length, count(*) FROM customer group by 1;


------- ALTER / Schema Evolution --------------------------------------------------------------------------------

ALTER TABLE customer ADD COLUMN phone varchar;

INSERT INTO customer (custkey, name, mktsegment, account_balance, nation, region, phone)
VALUES (200000, 'COMMANDER BUN BUN', 'SQLENGINE', 1, 'FRANCE', 'EUROPE','+33606060606');

SELECT * FROM customer ORDER BY name;

SELECT * FROM "customer$snapshots" ORDER BY committed_at DESC;

------- Partition Evolution ---------------------------------------------------------------------------------------

select * from customer;
SELECT * FROM "customer$partitions";

ALTER TABLE customer SET PROPERTIES partitioning = ARRAY['mktsegment', 'nation'];

INSERT INTO customer (custkey, name, mktsegment, account_balance, nation, region, phone) VALUES (200001, 'TRINO', 'SQLENGINE', 1, 'GALAXY', 'AMERICA','+15543023703');
INSERT INTO customer (custkey, name, mktsegment, account_balance, nation, region, phone) VALUES (200002, 'STARBURST', 'SQLENGINE', 2, 'GALAXY', 'AMERICA','+12822655571');

SELECT * FROM "customer$partitions";

SELECT * FROM "customer$manifests";

------- Time Travel / Snapshots -----------------------------------------------------------------------------------------

SELECT * FROM "customer$snapshots" ORDER BY committed_at DESC;

SELECT committed_at, snapshot_id, ROW_NUMBER() OVER (ORDER BY committed_at) AS snapshot_rank FROM "customer$snapshots" ORDER BY committed_at DESC;

SELECT * FROM customer order by custkey desc;

--Leverage Time Travel to view Diffs
SELECT * FROM customer as c
where c.custkey not in (select custkey from customer FOR VERSION AS OF 3484340040105408929)
ORDER BY c.custkey desc;

CALL system.rollback_to_snapshot('trino_iceberg_edu', 'customer', 3484340040105408929);

CALL system.rollback_to_snapshot('trino_iceberg_edu', 'customer', 770508368235808418);

SELECT * FROM customer order by custkey desc;

------- Optimize / Compaction - Vacuum / Cleaning snapshots and orphan files --------------------------

ALTER TABLE customer EXECUTE expire_snapshots(retention_threshold => '7d');

ALTER TABLE customer EXECUTE remove_orphan_files(retention_threshold => '7d');

--ALTER TABLE customer EXECUTE optimize(file_size_threshold => '10MB');

SELECT * FROM "customer$snapshots" ORDER BY committed_at DESC;

ALTER TABLE customer
  EXECUTE OPTIMIZE
  WHERE CAST(CURRENT_TIMESTAMP AS DATE) >= CAST(now() - INTERVAL '2' DAY AS DATE);

------- Get Columns Statistics for Trino CBO -----------------------------------------------------------

explain analyze customer;
SHOW STATS FOR customer;

------- Federation with Iceberg data --------------------------------------------------------------------------------

SELECT
    c.nation as nation,
    round(sum(o.totalprice)) as total_price
FROM
    customer c
    join demo_postgres_aws.demo.orders o on c.custkey = o.custkey
WHERE 
    c.region='AMERICA'
GROUP BY c.nation
ORDER BY total_price;