# CS 523 DE : Big Data Technology

## How to run ##
- Run all shell scripts under commands/ directory except 4-hive-confg.sh
- Build all java projects using: mvn clean package 
- java -jar target/producer-0.0.1-SNAPSHOT.jar
- java -jar target/consumer-0.0.1-SNAPSHOT.jar
- spark-submit --class cs523.bitcoinprice.hbasesparksql.BitcoinPriceSparkSql  --master local[*] target/hbasesparksql-1.0-SNAPSHOT.jar
- under commands directory run: ./4-hive-confg.sh
