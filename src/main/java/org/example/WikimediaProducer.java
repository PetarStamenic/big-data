package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

public class WikimediaProducer {
    static BufferedReader reader;
    public static void main(String[] args) throws IOException {
        String bootstrapServers = "localhost:9092"; // Update with your Kafka broker address
        String topic = "wikimedia_recent_changes"; // Update with your Kafka topic name
        String apiEndpoint = "https://stream.wikimedia.org/v2/stream/recentchange";

        // Kafka producer configuration
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,String.valueOf(32768));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        // Create Kafka producer

        Producer<String, String> producer = new KafkaProducer<>(properties);

        try {
            // Connect to the Wikimedia API and read the stream
            URL url = new URL(apiEndpoint);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;

            while (true) {
                try {
                    while ((line = reader.readLine()) != null) {
                        // Send the received data to Kafka
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
                        producer.send(record);
                        producer.flush();
                        System.out.println("Data sent to Kafka topic: " + topic);
                    }
                } catch (IOException e) {
                    Thread.sleep(500);
                }
            }
        }  catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            producer.close();
            reader.close();
        }
    }
}
