package cs523.bitcoinprice.producer;

public class BitcoinPrice {

	private String assetId;
    private double price;
    private String timestamp;
    private double size;

    // Getters and Setters

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public double getSize() {
        return size;
    }

    public void setSize(double size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "BitcoinPrice{" +
                "assetId='" + assetId + '\'' +
                ", price=" + price +
                ", timestamp='" + timestamp + '\'' +
                ", size=" + size +
                '}';
    }
}
