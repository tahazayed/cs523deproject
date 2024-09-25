package cs523.bitcoinprice.producer;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class BitcoinPriceProducer {

    // CoinAPI Key
    private static final String API_KEY = "";
    private static final String KAFKA_TOPIC = "bitcoin-price";
    private static final String KAFKA_BROKER = "localhost:9092";
    
    // Use UnsafeOkHttpClient
    private static final OkHttpClient client = UnsafeOkHttpClient.getUnsafeOkHttpClient();

    public static void main(String[] args) {
        // Kafka producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
             
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Create the WebSocket request to CoinAPI
        Request request = new Request.Builder()
                .url("wss://ws.coinapi.io/v1/")
                .addHeader("X-CoinAPI-Key", API_KEY)
                .build();

        WebSocketListener listener = new WebSocketListener() {
            @Override
            public void onOpen(WebSocket webSocket, okhttp3.Response response) {
                System.out.println("Connected to CoinAPI WebSocket!");
                // Optionally send a subscription message after connection
                webSocket.send("{ \"type\": \"hello\", \"apikey\": \"" + API_KEY + "\", \"heartbeat\": false, \"subscribe_data_type\": [\"trade\"], \"subscribe_filter_symbol_id\": [\"BITSTAMP_SPOT_BTC_USD\"] }");
            }

            @Override
            public void onMessage(WebSocket webSocket, String text) {
                // Log the raw message from the WebSocket
                System.out.println("Received message: " + text);

                try {
                    // Parse the JSON response into BitcoinPrice object
                    BitcoinPrice bitcoinPrice = parseBitcoinPriceData(text);

                    // Send Bitcoin price data to Kafka topic
                    if (bitcoinPrice != null) {
                        producer.send(new ProducerRecord<>(KAFKA_TOPIC, "bitcoin-price",new Gson().toJson(bitcoinPrice) ));
                        System.out.println("Sent to Kafka: " + bitcoinPrice.toString());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, okhttp3.Response response) {
                t.printStackTrace();
            }
        };

        // Start WebSocket connection
        client.newWebSocket(request, listener);
        client.dispatcher().executorService().shutdown();
    }

    // Method to parse JSON data into BitcoinPrice object
    private static BitcoinPrice parseBitcoinPriceData(String jsonText) {
        try {
            JSONObject jsonObject = new JSONObject(jsonText);

            // Adjusting the parsing logic to match the actual structure of the message
            if (jsonObject.has("symbol_id") && jsonObject.has("price")) {
                BitcoinPrice bitcoinPrice = new BitcoinPrice();
                bitcoinPrice.setAssetId(jsonObject.getString("symbol_id"));  // Using "symbol_id" for asset identification
                bitcoinPrice.setPrice(jsonObject.getDouble("price"));
                bitcoinPrice.setTimestamp(jsonObject.getString("time_exchange"));  // Using "time_exchange" for timestamp
                bitcoinPrice.setSize(jsonObject.getDouble("size"));  // Include trade size for additional information
                return bitcoinPrice;
            }

            System.out.println("Unexpected JSON structure: " + jsonText);
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
