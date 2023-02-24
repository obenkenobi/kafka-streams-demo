package com.example.kafkastreams.instrument.streamjob

import com.example.kafkastreams.constants.topics.outputTopicA
import com.example.kafkastreams.constants.topics.outputTopicB
import com.example.kafkastreams.constants.topics.outputTopicC
import com.newrelic.api.agent.NewRelic
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
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
fun main() {
    val streamsProps = Properties()
    FileInputStream("src/main/resources/streams.properties").use { fis -> streamsProps.load(fis) }
    streamsProps[StreamsConfig.APPLICATION_ID_CONFIG] = "output-streams"

    val builder = StreamsBuilder()

    val outputStreamA = builder.stream(
        outputTopicA,
        Consumed.with(Serdes.String(), Serdes.String())
    )
    val outputStreamB = builder.stream(
        outputTopicB,
        Consumed.with(Serdes.String(), Serdes.String())
    )
    val outputStreamC = builder.stream(
        outputTopicC,
        Consumed.with(Serdes.String(), Serdes.String())
    )


    outputStreamA
        .peek { key, value -> logRecord("Incoming output record [A]", key, value) }
    outputStreamB
        .peek { key, value -> logRecord("Incoming output record [B]", key, value) }
    outputStreamC
        .peek { key, value -> logRecord("Incoming output record [C]", key, value) }

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
    exitProcess(0)
}
