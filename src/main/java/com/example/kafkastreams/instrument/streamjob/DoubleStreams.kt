package com.example.kafkastreams.instrument.streamjob

import com.example.kafkastreams.constants.topics.*
import com.newrelic.api.agent.NewRelic
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.Stores
import java.io.FileInputStream
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

private fun logRecord(prompt: String?, key: String?, value: String?) {
    System.out.printf("txn: %s | %s - key: %s - value: %s\n",
        NewRelic.getAgent()?.transaction?.toString(), prompt, key, value)
}

@Throws(IOException::class)
fun runStreams(appId: String, clientId: String?, resourcesPath: String, inputTopic: String, outputTopic: String) {
    val streamsProps = Properties()
    FileInputStream(resourcesPath).use { fis ->
        streamsProps.load(fis)
    }

    streamsProps[StreamsConfig.APPLICATION_ID_CONFIG] = appId
    if (clientId != null) {
        streamsProps[StreamsConfig.CLIENT_ID_CONFIG] = clientId
    }

    val builder = StreamsBuilder()

    val streamA = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))

    streamA.peek { key, value -> logRecord("$appId|$clientId|Incoming from topic $inputTopic", key, value) }
        .peek { key, value -> logRecord("$appId|$clientId|Outgoing to topic $outputTopic", key, value) }
        .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()))


    KafkaStreams(builder.build(), streamsProps).use { kafkaStreams ->
        val shutdownLatch = CountDownLatch(1)
        Runtime.getRuntime().addShutdownHook(Thread {
            kafkaStreams.close(Duration.ofSeconds(2))
            shutdownLatch.countDown()
        })
        try {
            kafkaStreams.start()
            shutdownLatch.await()
        } catch (e: Throwable) {
            exitProcess(1)
        }
    }
}

@Throws(IOException::class)
fun main() {
    val thread1 = Thread {
        runStreams("d-streams-1", "client-1", "src/main/resources/streams.properties", inputTopicA, outputTopicA)
    }
    val thread2 = Thread {
        runStreams("d-streams-2", "client-2", "src/main/resources/streams.properties", inputTopicB, outputTopicB)
    }
    val threads = listOf(thread1, thread2)
    threads.forEach { it.start() }
    threads.forEach { it.join() }
    exitProcess(0)
}
