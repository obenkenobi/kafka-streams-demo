package com.example.kafkastreams.instrument;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class OutputStreams {
    public static void logRecord(String prompt, String key, String value) {
        System.out.printf("%s - key %s value %s %n", prompt, key, value);
    }

    public static void main(String[] args) throws IOException {
        Properties streamsProps = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            streamsProps.load(fis);
        }
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");

        StreamsBuilder builder = new StreamsBuilder();

        final String outputTopicA = streamsProps.getProperty("instrument_a.output.topic");
        final String outputTopicB = streamsProps.getProperty("instrument_b.output.topic");
        final String outputTopicC = streamsProps.getProperty("instrument_c.output.topic");

        KStream<String, String> outputStream_A = builder.stream(outputTopicA,
                Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> outputStream_B = builder.stream(outputTopicB,
                Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> outputStream_C = builder.stream(outputTopicC,
                Consumed.with(Serdes.String(), Serdes.String()));

        outputStream_A
                .peek((key, value) -> logRecord("Incoming output record [A]", key, value));
        outputStream_B
                .peek((key, value) -> logRecord("Incoming output record [B]", key, value));
        outputStream_C
                .peek((key, value) -> logRecord("Incoming output record [C]", key, value));


        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
//            TopicLoader.runProducer();
            try {
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}
