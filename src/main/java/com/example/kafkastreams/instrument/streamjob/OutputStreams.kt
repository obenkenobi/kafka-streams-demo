package com.example.kafkastreams.instrument.streamjob

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
    System.out.printf("%s - key %s value %s %n", prompt, key, value)
}

@Throws(IOException::class)
fun main() {
    val streamsProps = Properties()
    FileInputStream("src/main/resources/streams.properties").use { fis -> streamsProps.load(fis) }
    streamsProps[StreamsConfig.APPLICATION_ID_CONFIG] = "output-streams"

    val builder = StreamsBuilder()
    val outputTopicA = streamsProps.getProperty("instrument_a.output.topic")
    val outputTopicB = streamsProps.getProperty("instrument_b.output.topic")
    val outputTopicC = streamsProps.getProperty("instrument_c.output.topic")

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
