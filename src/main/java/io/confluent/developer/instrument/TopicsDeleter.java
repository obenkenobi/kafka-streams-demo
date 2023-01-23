package io.confluent.developer.instrument;

import io.confluent.developer.StreamsUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class TopicsDeleter {

    public static void main(String[] args) throws IOException {
        runDeleter();
    }

    public static void runDeleter() throws IOException {
        Properties properties = StreamsUtils.loadProperties();

        try(Admin adminClient = Admin.create(properties)) {

            final String inputTopicA = properties.getProperty("instrument_a.input.topic");
            final String inputTopicB = properties.getProperty("instrument_b.input.topic");
            final String inputTopicC = properties.getProperty("instrument_c.input.topic");
            final String inputTopicD = properties.getProperty("instrument_d.input.topic");
            final String inputTopicE = properties.getProperty("instrument_e.input.topic");
            final String outputTopicA = properties.getProperty("instrument_a.output.topic");
            final String outputTopicB = properties.getProperty("instrument_b.output.topic");
            final String outputTopicC = properties.getProperty("instrument_c.output.topic");

            var topics = List.of(inputTopicA, inputTopicB, inputTopicD,
                    inputTopicC, inputTopicE, outputTopicA, outputTopicB, outputTopicC);
            adminClient.deleteTopics(topics);
        }
    }
}

