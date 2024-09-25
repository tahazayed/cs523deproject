-- Create a Hive database if it doesn't exist
CREATE DATABASE IF NOT EXISTS bitcoin_data;

-- Switch to the created database
USE bitcoin_data;

DROP TABLE IF EXISTS bitcoin_price_hbase;

-- Create an external Hive table that maps to an existing HBase table
CREATE EXTERNAL TABLE IF NOT EXISTS bitcoin_price_hbase (
    asset_id STRING,       -- Row key (maps to HBase row key)
    price DOUBLE,          -- Maps to HBase column 'price-info:price' (binary double)
    timestamp BIGINT,      -- Maps to HBase column 'price-info:timestamp' (binary long as UNIX epoch time)
    size DOUBLE            -- Maps to HBase column 'price-info:size' (binary double)
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,price-info:price#b,price-info:timestamp#b,price-info:size#b",
    'serialization.format' = '1'
)
TBLPROPERTIES ("hbase.table.name" = "bitcoin_price");

-- Check if the table was created correctly
SHOW TABLES;

-- Describe the table to ensure correct column mappings
DESCRIBE bitcoin_price_hbase;

-- Select data from the HBase-backed Hive table and keep the timestamp column for sorting
SELECT asset_id, price, timestamp, from_unixtime(CAST(timestamp / 1000 AS BIGINT)) AS readable_timestamp, size
FROM bitcoin_price_hbase
ORDER BY timestamp DESC
LIMIT 10;

