package cs523.bitcoinprice.consumer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class BitcoinPriceListener {
    private static final Logger logger = LoggerFactory.getLogger(BitcoinPriceListener.class);

    // Process method remains unchanged, but using JavaPairInputDStream instead of JavaInputDStream
    public static void process(JavaPairInputDStream<String, String> stream) {
        JavaDStream<String> bitcoinPrices = stream.map(Tuple2::_2); // Extract the values from the Tuple2

        bitcoinPrices.foreachRDD(rdd -> {
            rdd.foreach(record -> {
                try {
                    // Parse the record as JSON using Gson
                    JsonObject jsonObject = JsonParser.parseString(record).getAsJsonObject();

                    // Extract fields from the JSON
                    String assetId = jsonObject.get("assetId").getAsString();
                    double price = jsonObject.get("price").getAsDouble();
                    String timestamp = jsonObject.get("timestamp").getAsString();
                    double size = jsonObject.get("size").getAsDouble();

                    // Log and process the Bitcoin price data
                    logger.info("Processed Bitcoin Price: assetId={}, price={}, timestamp={}, size={}",
                            assetId, price, timestamp, size);

                    BitcoinPrice bitcoinPrice = new BitcoinPrice(assetId, price, timestamp, size);

                    BitcoinPriceHbaseTable.populateData(bitcoinPrice);

                } catch (Exception e) {
                    // Handle JSON parsing error
                    logger.error("Invalid JSON received: " + record, e);
                }
            });
        });
    }

    public static void main(String[] args) throws InterruptedException {
        // Spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("BitcoinPriceListener")
                .setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        // Kafka parameters
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("group.id", "bitcoin-price-consumer-group");
        kafkaParams.put("auto.offset.reset", "largest");

        // Topics
        Set<String> topics = new HashSet<>(Arrays.asList("bitcoin-price"));

        // Create direct Kafka stream using the older API (Kafka 0.8/0.10 API)
        JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(
                streamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );

        // Process the Kafka stream
        process(stream);

        // Start streaming
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
