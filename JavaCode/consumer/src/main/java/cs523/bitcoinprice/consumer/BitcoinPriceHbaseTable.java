package cs523.bitcoinprice.consumer;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;
import java.time.Instant;

public class BitcoinPriceHbaseTable {

    private static final String TABLE_NAME = "bitcoin_price";
    private static final String COLUMN_FAMILY = "price-info";

    private static Connection connection = null;

    static {
        try {
            // Initialize HBase configuration and connection
            Configuration config = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(config);

            // Ensure table exists
            createTableIfNotExists();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to create the table if it does not exist
    private static void createTableIfNotExists() throws IOException {
        Admin admin = connection.getAdmin();

        // Check if the table exists
        TableName tableName = TableName.valueOf(TABLE_NAME);
        if (!admin.tableExists(tableName)) {
            System.out.println("Table does not exist. Creating table: " + TABLE_NAME);

            // Create a table descriptor and column family descriptor
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor columnFamilyDescriptor = new HColumnDescriptor(COLUMN_FAMILY);
            tableDescriptor.addFamily(columnFamilyDescriptor);

            // Create the table
            admin.createTable(tableDescriptor);
            System.out.println("Table created successfully: " + TABLE_NAME);
        } else {
            System.out.println("Table already exists: " + TABLE_NAME);
        }

        admin.close();
    }

    // Method to insert BitcoinPrice into HBase
    public static void populateData(BitcoinPrice bitcoinPrice) {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            // Create a composite row key using assetId and timestamp
            String rowKey = createCompositeRowKey(bitcoinPrice.getAssetId(), bitcoinPrice.getTimestamp());

            // Create a new row in HBase with the composite row key
            Put put = new Put(Bytes.toBytes(rowKey));

            // Add columns for the Bitcoin price data
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("price"), Bytes.toBytes(bitcoinPrice.getPrice()));

            // Convert timestamp to UNIX epoch time in milliseconds and store it as long
            long timestampMillis = Instant.parse(bitcoinPrice.getTimestamp()).toEpochMilli();
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("timestamp"), Bytes.toBytes(timestampMillis));

            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("size"), Bytes.toBytes(bitcoinPrice.getSize()));

            // Store the row in the HBase table
            table.put(put);
            System.out.println("Inserted BitcoinPrice into HBase: " + bitcoinPrice);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to create a composite row key using assetId and timestamp
    private static String createCompositeRowKey(String assetId, String timestamp) {
        // Combine assetId and timestamp to create a unique row key
        return assetId + "_" + timestamp;
    }

    // Close HBase connection when done
    public static void closeConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
