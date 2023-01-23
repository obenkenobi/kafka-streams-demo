package com.example.kafkastreams;

import org.apache.kafka.clients.admin.NewTopic;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class StreamsUtils {

    public static final String PROPERTIES_FILE_PATH = "src/main/resources/streams.properties";
    public static final short REPLICATION_FACTOR = 3;
    public static final int PARTITIONS = 6;

    public static Properties loadProperties() throws IOException {
            Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(PROPERTIES_FILE_PATH)) {
            properties.load(fis);
            return properties;
        }
    }
    public static NewTopic createTopic(final String topicName){
              return new NewTopic(topicName, PARTITIONS, REPLICATION_FACTOR);
    }
}
