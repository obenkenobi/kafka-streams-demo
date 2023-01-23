package io.confluent.developer.instrument;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class PrimaryStreams {

    public static void logRecord(String prompt, String key, String value) {
        System.out.printf("%s - key %s value %s\n", prompt, key, value);
    }

    public static void main(String[] args) throws IOException {
        Properties streamsProps = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            streamsProps.load(fis);
        }
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");

        StreamsBuilder builder = new StreamsBuilder();

        final String inputTopicA = streamsProps.getProperty("instrument_a.input.topic");
        final String inputTopicB = streamsProps.getProperty("instrument_b.input.topic");
        final String inputTopicC = streamsProps.getProperty("instrument_c.input.topic");
        final String inputTopicD = streamsProps.getProperty("instrument_d.input.topic");
        final String inputTopicE = streamsProps.getProperty("instrument_e.input.topic");
        final String outputTopicA = streamsProps.getProperty("instrument_a.output.topic");
        final String outputTopicB = streamsProps.getProperty("instrument_b.output.topic");
        final String outputTopicC = streamsProps.getProperty("instrument_c.output.topic");

        KStream<String, String> stream_A = builder.stream(inputTopicA, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> stream_B = builder.stream(inputTopicB, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> stream_E = builder.stream(inputTopicE, Consumed.with(Serdes.String(), Serdes.String()));

        KeyValueBytesStoreSupplier storeSupplierC = Stores.inMemoryKeyValueStore("ktable-C-store");
        KTable<String, String> kTable_C = builder.table(inputTopicC,
                Materialized.<String, String>as(storeSupplierC)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        KeyValueBytesStoreSupplier storeSupplierD = Stores.inMemoryKeyValueStore("ktable-D-store");
        KTable<String, String> kTable_D = builder.table(inputTopicD,
                Materialized.<String, String>as(storeSupplierD)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        KStream<String, String> processedStream_A = stream_A
                .peek((key, value) -> logRecord("Incoming streamA", key, value))
                .filter((key, value) -> value.contains("1"))
                .mapValues(value -> value.toUpperCase())
                .peek((key, value) -> logRecord("Processed streamA", key, value));

        KStream<String, String> processedStream_B = stream_B
                .peek((key, value) -> logRecord("Incoming streamB", key, value))
                .filter((key, value) -> value.contains("1"))
                .mapValues(value -> value.toUpperCase())
                .peek((key, value) -> logRecord("Processed streamB", key, value));

        KTable<String, String> processedKTable_C = kTable_C
                .filter((key, value) -> value.contains("1"))
                .mapValues(value -> value.toUpperCase());

        KStream<String, String> joinStream_A_B = processedStream_A.join(processedStream_B,
                (a, b) -> a + b,
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(30), Duration.ofMinutes(30)),
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()));

        KStream<String, String> joinStream_A_B_C = joinStream_A_B.join(processedKTable_C,
                (a, b) -> a + b,
                Joined.with(Serdes.String(), Serdes.String(), Serdes.String()));

        joinStream_A_B_C
                .peek((key, value) -> logRecord("Outgoing topic A joinStream_A_B_C record", key, value))
                .to(outputTopicA, Produced.with(Serdes.String(), Serdes.String()));

        kTable_D.mapValues(value -> value.toUpperCase())
                .toStream()
                .peek((key, value) -> logRecord("Outgoing kTable D record to topic B", key, value))
                .to(outputTopicB, Produced.with(Serdes.String(), Serdes.String()));

        KeyValueBytesStoreSupplier storeSupplierReduce_E =
                Stores.inMemoryKeyValueStore("reducer-E-store");
        stream_E
                .peek((key, value) -> logRecord("Incoming streamE", key, value))
                .mapValues(value -> value != null ? value.trim().length() : 0)
                .groupByKey()
                .reduce((Integer::sum), Materialized.<String, Integer>as(storeSupplierReduce_E)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Integer()))
                .toStream()
                .mapValues(String::valueOf)
                .peek((key, value) -> logRecord("Outgoing topic C stream_E record", key, value))
                .to(outputTopicC, Produced.with(Serdes.String(), Serdes.String()));

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