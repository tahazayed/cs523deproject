package cs523.bitcoinprice.hbasesparksql;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class BitcoinPriceSparkSql {

    public static void main(String[] args) {
        // Set up Spark configuration for Spark 1.6.0
        SparkConf conf = new SparkConf().setAppName("BitcoinPriceSparkSQL").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // HBase configuration
        org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "localhost:2181"); // Set correct Zookeeper address

        // Define schema for DataFrame
        StructType schema = new StructType()
                .add("assetId", DataTypes.StringType)
                .add("price", DataTypes.DoubleType)
                .add("timestamp", DataTypes.LongType)  // Store timestamp as LONG (UNIX epoch time)
                .add("size", DataTypes.DoubleType);

        List<org.apache.spark.sql.Row> data = new ArrayList<>();

        // Retrieve data from HBase
        try (Connection connection = ConnectionFactory.createConnection(hbaseConf);
             Table table = connection.getTable(TableName.valueOf("bitcoin_price"))) {

            Scan scan = new Scan();
            for (Result result : table.getScanner(scan)) {
                String assetId = Bytes.toString(result.getRow()); // Assuming assetId is the row key
                double price = Bytes.toDouble(result.getValue(Bytes.toBytes("price-info"), Bytes.toBytes("price")));

                // Read the timestamp as a long (UNIX epoch time)
                long timestampMillis = Bytes.toLong(result.getValue(Bytes.toBytes("price-info"), Bytes.toBytes("timestamp")));

                // Directly store the timestamp as long
                double size = Bytes.toDouble(result.getValue(Bytes.toBytes("price-info"), Bytes.toBytes("size")));

                // Add the row data with the long timestamp
                data.add(RowFactory.create(assetId, price, timestampMillis, size));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Create DataFrame from the list of rows
        DataFrame df = sqlContext.createDataFrame(data, schema);
        df.registerTempTable("bitcoin_price");

        // Perform SQL queries on the DataFrame
        DataFrame query1 = sqlContext.sql("SELECT AVG(price) as avg_price FROM bitcoin_price");
        query1.show();

        DataFrame query2 = sqlContext.sql("SELECT SUM(size) as total_size FROM bitcoin_price");
        query2.show();

        // Stop the Spark context
        sc.stop();
    }
}
