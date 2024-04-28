package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class WikimediaConsumer {

    private static int fileNumber = 1;
    public static void main(String[] args) {
        String topicName = "wikimedia_recent_changes"; // Replace with your topic name
        String bootstrapServers = "localhost:9092"; // Replace with your Kafka server address
        String groupId = "wiki-group"; // Replace with your group ID

        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", groupId);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList(topicName));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() != null && record.value().startsWith("data: ")) {
                        // This is a data message, process it
                        System.out.printf("Offset = %d, Key = %s, Value = %s\n", record.offset(), record.key(), record.value());
                        writeToCSV(new JSONObject(record.value().substring(6)));
                    } else {
                        // This is likely a control message, ignore or handle separately
                        System.out.println("Control message received, ignoring...");
                    }
                }
            }
        }
    }
    private static void writeToCSV(JSONObject jsonObject) {
        try (FileWriter fileWriter = new FileWriter("output"+fileNumber+".csv", true)) {
            String id = jsonObject.optString("id");
            if(id == null || id.isEmpty())
                return;
            id = id.replace(","," ").replace(";"," ");
            String type = jsonObject.optString("type").replace(","," ").replace(";"," ");
            int namespace = jsonObject.optInt("namespace");
            String title = jsonObject.optString("title").replace(","," ").replace(";"," ");
            String titleUrl = jsonObject.optString("title_url").replace(","," ").replace(";"," ");
            String comment = jsonObject.optString("comment").replace(","," ").replace(";"," ").replace("\\n"," ");
            long timestamp = jsonObject.optLong("timestamp");
            String user = jsonObject.optString("user").replace(","," ").replace(";"," ");
            boolean bot = jsonObject.optBoolean("bot");
            String notifyUrl = jsonObject.optString("notify_url");
            String serverUrl = jsonObject.optString("server_url");
            String serverName = jsonObject.optString("server_name");
            String serverScriptPath = jsonObject.optString("server_script_path");
            String wiki = jsonObject.optString("wiki").replace(","," ").replace(";"," ");
            String parsedComment = jsonObject.optString("parsedcomment");

            String csvLine = "";
            switch (fileNumber){
                case 0:{
                    csvLine = String.join(",", id, type, title, comment, user, String.valueOf(bot));
                    break;
                }
                case 1:{
                    csvLine = String.join(",", id,String.valueOf(timestamp), user, String.valueOf(bot), wiki);
                    break;
                }
            }


            fileWriter.append(csvLine).append("\n");
        } catch (IOException e) {
            System.out.println("Error writing to CSV: " + e.getMessage());
        }
    }
    private static void writeRecordToFile(ConsumerRecord<String, String> record) {
        try (FileWriter fileWriter = new FileWriter("wikimedia_changes.csv", true)) {
            fileWriter.append(String.format("%d,%s,%s\n", record.offset(), record.key(), record.value()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}