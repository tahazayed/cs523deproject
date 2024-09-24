-- Create a Hive database (if needed)
CREATE DATABASE IF NOT EXISTS bitcoin_data;

-- Use the created database
USE bitcoin_data;

-- Create a Hive table for Bitcoin price data
CREATE TABLE IF NOT EXISTS bitcoin_price (
    asset_id STRING,       -- assetId in BitcoinPrice
    price DOUBLE,          -- price in BitcoinPrice
    timestamp STRING,      -- timestamp in BitcoinPrice
    size DOUBLE            -- size in BitcoinPrice
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Load initial data into the table (optional, if you have a CSV or other source)
-- Example of loading data from HDFS, if data is already stored in HDFS
-- LOAD DATA INPATH '/user/hadoop/bitcoin_price_data.csv' INTO TABLE bitcoin_price;

-- Example query: Show the top 10 latest prices for Bitcoin
SELECT * FROM bitcoin_price
WHERE asset_id = 'BITSTAMP_SPOT_BTC_USD'  -- Replace with the actual symbol_id if needed
ORDER BY timestamp DESC
LIMIT 10;

-- Example: Calculate the average Bitcoin price over a specific time period
SELECT AVG(price) AS avg_price FROM bitcoin_price
WHERE asset_id = 'BITSTAMP_SPOT_BTC_USD'  -- Replace with the actual symbol_id if needed
  AND timestamp BETWEEN '2024-01-01' AND '2024-12-31';

-- Example: Show the total volume of Bitcoin traded in a specific time period
SELECT SUM(size) AS total_volume FROM bitcoin_price
WHERE asset_id = 'BITSTAMP_SPOT_BTC_USD'  -- Replace with the actual symbol_id if needed
  AND timestamp BETWEEN '2024-01-01' AND '2024-12-31';

