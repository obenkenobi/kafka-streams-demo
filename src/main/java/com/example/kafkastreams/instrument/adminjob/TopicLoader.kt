package com.example.kafkastreams.instrument.adminjob

import com.example.kafkastreams.loadProperties
import com.example.kafkastreams.createTopic
import com.newrelic.api.agent.NewRelic
import com.newrelic.api.agent.Trace
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.StringSerializer
import java.io.IOException
import java.util.function.Consumer

fun main() {
    runProducer()
}

@Throws(IOException::class, InterruptedException::class)
fun runProducer() {
    val properties = loadProperties()
    properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    val callback = Callback { metadata: RecordMetadata, exception: Exception? ->
        if (exception != null) {
            System.out.printf("Producing records encountered error %s %n", exception)
        } else {
            System.out.printf(
                "Record produced | topic - %s offset - %d timestamp - %d %n",
                metadata.topic(), metadata.offset(), metadata.timestamp()
            )
        }
    }
    Admin.create(properties).use { adminClient ->
        KafkaProducer<String, String>(properties).use { producer ->
            val inputTopicA = properties.getProperty("instrument_a.input.topic")
            val inputTopicB = properties.getProperty("instrument_b.input.topic")
            val inputTopicC = properties.getProperty("instrument_c.input.topic")
            val inputTopicD = properties.getProperty("instrument_d.input.topic")
            val inputTopicE = properties.getProperty("instrument_e.input.topic")
            val outputTopicA = properties.getProperty("instrument_a.output.topic")
            val outputTopicB = properties.getProperty("instrument_b.output.topic")
            val outputTopicC = properties.getProperty("instrument_c.output.topic")
            val topics = listOf(
                createTopic(inputTopicA),
                createTopic(inputTopicB),
                createTopic(inputTopicC),
                createTopic(inputTopicD),
                createTopic(inputTopicE),
                createTopic(outputTopicA),
                createTopic(outputTopicB),
                createTopic(outputTopicC)
            )
            adminClient.createTopics(topics)
            val eventsA = listOf(
                KVPair(1L, "a_1"),
//                    KVPair(2L, "a_2"),
//                    KVPair(3L, "a_3"),
//                    KVPair(4L, "a_4"),
            )
            val eventsB = listOf(
                KVPair(1L, "b_1"),
//                    KVPair(2L, "b_2"),
//                    KVPair(3L, "b_3"),
//                    KVPair(4L, "b_4"),
            )
            val eventsC = listOf(
                KVPair(1L, "c_1"),
//                    KVPair(2L, "c_2"),
//                    KVPair(3L, "c_3"),
//                    KVPair(4L, "c_4"),
            )
            val eventsD = listOf(
                KVPair(1L, "d_1"),
//                    KVPair(2L, "d_2"),
//                    KVPair(3L, "d_3"),
//                    KVPair(4L, "d_4"),
            )
            val eventsE = listOf(
                KVPair(1L, "e_1"),
//                    KVPair(2L, "e_2"),
//                    KVPair(3L, "e_3"),
//                    KVPair(4L, "e_4"),
            )
            eventsA.forEach(Consumer { pair: KVPair<Long, String> ->
                sendRecord(producer, inputTopicA, pair, callback)
            })
            eventsB.forEach(Consumer { pair: KVPair<Long, String> ->
                sendRecord(producer, inputTopicB, pair, callback)
            })
            eventsC.forEach(Consumer { pair: KVPair<Long, String> ->
                sendRecord(producer, inputTopicC, pair, callback)
            })
            eventsD.forEach(Consumer { pair: KVPair<Long, String> ->
                sendRecord(producer, inputTopicD, pair, callback)
            })
            eventsE.forEach(Consumer { pair: KVPair<Long, String> -> sendRecord(producer, inputTopicE, pair, callback) })
        }
    }
    Thread.sleep(60000)
}

@Trace(dispatcher = true)
fun sendRecord(
    producer: Producer<String, String>,
    topic: String?, recordPair: KVPair<Long, String>,
    callback: Callback?
) {
    NewRelic.setTransactionName("kafkaProducer", String.format("MessageBroker/Kafka/Topic/Produce/Named/%s", topic))
    val producerRecord = ProducerRecord<String, String>(topic, recordPair.key.toString(), recordPair.value)
    producer.send(producerRecord, callback)
}

class KVPair<K, V> constructor(val key: K, val value: V) {
}
