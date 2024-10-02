package com.example.kafkastreams.topicmanager

import com.example.kafkastreams.constants.topics.*
import com.example.kafkastreams.loadProperties
import com.example.kafkastreams.createTopic
import com.newrelic.api.agent.NewRelic
import com.newrelic.api.agent.Trace
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.StringSerializer
import java.io.IOException

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
            adminClient.createTopics(topics).values().entries.parallelStream().forEach {
                try {
                    it.value.get()
                } catch (e: Throwable) {
                    println(e.message)
                }
            }

            val recordsPerTopic = 1000000L
            (1L..recordsPerTopic).flatMap {
                listOf(
                    KVPair(it, "a_${it}_${System.currentTimeMillis()}", inputTopicA),
                    KVPair(it, "b_${it}_${System.currentTimeMillis()}", inputTopicB),
                    KVPair(it, "c_${it}_${System.currentTimeMillis()}", inputTopicC),
                    KVPair(it, "d_${it}_${System.currentTimeMillis()}", inputTopicD),
                    KVPair(it, "e_${it}_${System.currentTimeMillis()}", inputTopicE))
            }.parallelStream().forEach {
                sendRecord(producer, it, callback)
            }
        }
    }
}


fun sendRecord(producer: Producer<String, String>, pair: KVPair<Long, String>, callback: Callback?) {
    println("Sending | topic: ${pair.topic} - key: ${pair.key}")
    val producerRecord = ProducerRecord(pair.topic, pair.key.toString(), pair.value)
    producer.send(producerRecord, callback)
}

class KVPair<K, V> constructor(val key: K, val value: V, val topic: String) {
}
