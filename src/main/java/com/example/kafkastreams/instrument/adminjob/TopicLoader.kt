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
import java.util.stream.IntStream

fun main() {
    runProducer()
}

@Trace(dispatcher = true)
@Throws(IOException::class, InterruptedException::class)
fun runProducer() {
    NewRelic.setTransactionName("kafkaProducer", "MessageBroker/Kafka/Topic/Produce")
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
            val numRecordsPerTopic = 100000L
            adminClient.createTopics(topics)
            val eventsA = (1L..numRecordsPerTopic).map { KVPair(it, "a_${it}")}

            val eventsB = (1L..numRecordsPerTopic).map { KVPair(it, "b_${it}")}
            val eventsC = (1L..numRecordsPerTopic).map { KVPair(it, "c_${it}")}
            val eventsD = (1L..numRecordsPerTopic).map { KVPair(it, "d_${it}")}
            val eventsE = (1L..numRecordsPerTopic).map { KVPair(it, "e_${it}")}

            eventsA.parallelStream().forEach { pair: KVPair<Long, String> ->
                sendRecord(producer, inputTopicA, pair, callback)
            }
            eventsB.parallelStream().forEach { pair: KVPair<Long, String> ->
                sendRecord(producer, inputTopicB, pair, callback)
            }
            eventsC.parallelStream().forEach { pair: KVPair<Long, String> ->
                sendRecord(producer, inputTopicC, pair, callback)
            }
            eventsD.parallelStream().forEach { pair: KVPair<Long, String> ->
                sendRecord(producer, inputTopicD, pair, callback)
            }
            eventsE.parallelStream().forEach { pair: KVPair<Long, String> ->
                sendRecord(producer, inputTopicE, pair, callback) }
        }
    }
    Thread.sleep(60000)
}


fun sendRecord(
    producer: Producer<String, String>,
    topic: String?, recordPair: KVPair<Long, String>,
    callback: Callback?
) {
    println("Sending | topic: $topic - key: ${recordPair.key}")
    val producerRecord = ProducerRecord(topic, recordPair.key.toString(), recordPair.value)
    producer.send(producerRecord, callback)
}

class KVPair<K, V> constructor(val key: K, val value: V) {
}
