spark-submit \
  --class cs523.bitcoinprice.hbasesparksql.BitcoinPriceSparkSql \
  --master local[*] \
  target/hbasesparksql-1.0-SNAPSHOT.jar
