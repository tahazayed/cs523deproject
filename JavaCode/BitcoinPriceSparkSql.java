package cs523.bitcoinprice.consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class BitcoinPriceSparkSql {

    private static final String TABLE_NAME = "bitcoin_price";
    private static final String CF_PRICE_INFO = "price-info";
    
    static Configuration config;
    static JavaSparkContext jsc;
    
    public static void main(String[] args) {

        // Initialize Spark configuration
        SparkConf conf = new SparkConf().setAppName("BitcoinPriceSQL").setMaster("local[*]");
        conf.registerKryoClasses(new Class[]{org.apache.hadoop.hbase.io.ImmutableBytesWritable.class});
        
        // Set up HBase configuration
        config = HBaseConfiguration.create();
        config.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);

        // Initialize Spark context
        jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc.sc());
        
        // Read HBase table as JavaPairRDD
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = readTableByJavaPairRDD();
        System.out.println("Number of rows in HBase table: " + hBaseRDD.count());
        
        // Map HBase results to BitcoinPrice objects
        JavaRDD<BitcoinPrice> rows = hBaseRDD.map(x -> {
            BitcoinPrice bitcoinPrice = new BitcoinPrice();
            bitcoinPrice.setAssetId(Bytes.toString(x._1.get()));  // Assuming assetId is part of the row key
            bitcoinPrice.setPrice(Bytes.toDouble(x._2.getValue(Bytes.toBytes(CF_PRICE_INFO), Bytes.toBytes("price"))));
            bitcoinPrice.setTimestamp(Bytes.toString(x._2.getValue(Bytes.toBytes(CF_PRICE_INFO), Bytes.toBytes("timestamp"))));
            bitcoinPrice.setSize(Bytes.toDouble(x._2.getValue(Bytes.toBytes(CF_PRICE_INFO), Bytes.toBytes("size"))));

            return bitcoinPrice;
        });

        // Convert RDD to DataFrame
        DataFrame tabledata = sqlContext.createDataFrame(rows, BitcoinPrice.class);
        tabledata.registerTempTable(TABLE_NAME);  // Registering as a temp table for SQL queries
        tabledata.printSchema();

        // Example query 1: Find the average price grouped by assetId
        DataFrame query1 = sqlContext.sql("SELECT assetId, AVG(price) as avg_price FROM bitcoin_price GROUP BY assetId");
        query1.show();

        // Example query 2: Find the total transaction size per assetId
        DataFrame query2 = sqlContext.sql("SELECT assetId, SUM(size) as total_size FROM bitcoin_price GROUP BY assetId");
        query2.show();

        // Stop the Spark context
        jsc.stop();
    }

    // Method to read the HBase table as a JavaPairRDD
    public static JavaPairRDD<ImmutableBytesWritable, Result> readTableByJavaPairRDD() {
        return jsc.newAPIHadoopRDD(
                config,
                TableInputFormat.class,
                org.apache.hadoop.hbase.io.ImmutableBytesWritable.class,
                org.apache.hadoop.hbase.client.Result.class
        );
    }
}
