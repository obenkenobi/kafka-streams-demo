package io.confluent.developer.instrument;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.TransactionNamePriority;
import io.confluent.developer.StreamsUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class TopicLoader {

    public static void main(String[] args) throws IOException, InterruptedException {
        runProducer();
    }

    private static class KVPair<K, V> {
        private final K key;
        private final V value;

        public static <T1, T2> KVPair<T1, T2> of(T1 key, T2 value) {
            return new KVPair<>(key, value);
        }
        private KVPair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }

    public static void runProducer() throws IOException, InterruptedException {
        Properties properties = StreamsUtils.loadProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Callback callback = (metadata, exception) -> {
            if(exception != null) {
                System.out.printf("Producing records encountered error %s %n", exception);
            } else {
                System.out.printf("Record produced | topic - %s offset - %d timestamp - %d %n",
                        metadata.topic(), metadata.offset(), metadata.timestamp());
            }
        };

        try(Admin adminClient = Admin.create(properties);
            Producer<String, String> producer = new KafkaProducer<>(properties)) {

            final String inputTopicA = properties.getProperty("instrument_a.input.topic");
            final String inputTopicB = properties.getProperty("instrument_b.input.topic");
            final String inputTopicC = properties.getProperty("instrument_c.input.topic");
            final String inputTopicD = properties.getProperty("instrument_d.input.topic");
            final String inputTopicE = properties.getProperty("instrument_e.input.topic");
            final String outputTopicA = properties.getProperty("instrument_a.output.topic");
            final String outputTopicB = properties.getProperty("instrument_b.output.topic");
            final String outputTopicC = properties.getProperty("instrument_c.output.topic");

            var topics = List.of(StreamsUtils.createTopic(inputTopicA),
                                                StreamsUtils.createTopic(inputTopicB),
                                                StreamsUtils.createTopic(inputTopicC),
                                                StreamsUtils.createTopic(inputTopicD),
                                                StreamsUtils.createTopic(inputTopicE),
                                                StreamsUtils.createTopic(outputTopicA),
                                                StreamsUtils.createTopic(outputTopicB),
                                                StreamsUtils.createTopic(outputTopicC));
            adminClient.createTopics(topics);

            List<KVPair<Long, String>> eventsA = List.of(
                    KVPair.of(1L, "a_1"),
                    KVPair.of(2L, "a_2"),
                    KVPair.of(3L, "a_3"),
                    KVPair.of(4L, "a_4")
            );
            List<KVPair<Long, String>> eventsB = List.of(
                    KVPair.of(1L, "b_1"),
                    KVPair.of(2L, "b_2"),
                    KVPair.of(3L, "b_3"),
                    KVPair.of(4L, "b_4"));
            List<KVPair<Long, String>> eventsC = List.of(
                    KVPair.of(1L, "c_1"),
                    KVPair.of(2L, "c_2"),
                    KVPair.of(3L, "c_3"),
                    KVPair.of(4L, "c_4"));

            List<KVPair<Long, String>> eventsD = List.of(
                    KVPair.of(1L, "d_1"),
                    KVPair.of(2L, "d_2"),
                    KVPair.of(3L, "d_3"),
                    KVPair.of(4L, "d_4"));

            List<KVPair<Long, String>> eventsE = List.of(
                    KVPair.of(1L, "d_1"),
                    KVPair.of(2L, "d_2"),
                    KVPair.of(3L, "d_3"),
                    KVPair.of(4L, "d_4"));

            eventsA.forEach(pair -> {
                sendRecord(producer, inputTopicA, pair, callback);
            });
            eventsB.forEach(pair -> {
                sendRecord(producer, inputTopicB, pair, callback);
            });
            eventsC.forEach(pair -> {
                sendRecord(producer, inputTopicC, pair, callback);
            });
            eventsD.forEach(pair -> {
                sendRecord(producer, inputTopicD, pair, callback);
            });
            eventsE.forEach(pair -> {
                sendRecord(producer, inputTopicE, pair, callback);
            });
        }

        Thread.sleep(Duration.ofMinutes(5).toMillis());
    }

    @Trace(dispatcher = true)
    public static void sendRecord(Producer<String, String> producer,
            String topic, KVPair<Long, String> recordPair, Callback callback) {
                NewRelic.setTransactionName("kafkaProducer",
                String.format("MessageBroker/Kafka/Topic/Produce/Named/%s", topic));


        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, recordPair.getKey().toString(), recordPair.getValue());

        producer.send(producerRecord, callback);
    }
}

