-- Create a Hive database if it doesn't exist
CREATE DATABASE IF NOT EXISTS bitcoin_data;

-- Switch to the created database
USE bitcoin_data;

-- Create an external Hive table that maps to an existing HBase table
CREATE EXTERNAL TABLE IF NOT EXISTS bitcoin_price_hbase (
    asset_id STRING,       -- Row key (maps to HBase row key)
    price DOUBLE,          -- Maps to HBase column 'price-info:price'
    timestamp STRING,      -- Maps to HBase column 'price-info:timestamp'
    size DOUBLE            -- Maps to HBase column 'price-info:size'
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,price-info:price,price-info:timestamp,price-info:size"
)
TBLPROPERTIES ("hbase.table.name" = "bitcoin_price");

-- Select data from the HBase-backed Hive table
SELECT * FROM bitcoin_price_hbase
ORDER BY timestamp DESC
LIMIT 10;

-- Example: Calculate the average Bitcoin price 
SELECT AVG(price) AS avg_price FROM bitcoin_price_hbase;

-- Example: Show the total volume of Bitcoin traded 
SELECT SUM(size) AS total_volume FROM bitcoin_price_hbase;
