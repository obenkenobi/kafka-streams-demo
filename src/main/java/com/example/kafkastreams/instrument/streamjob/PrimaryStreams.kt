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
fun main() {
    val streamsProps = Properties()
    FileInputStream("src/main/resources/streams.properties").use { fis ->
        streamsProps.load(fis)
    }

    streamsProps[StreamsConfig.APPLICATION_ID_CONFIG] = "primary-streams"
    streamsProps[StreamsConfig.CLIENT_ID_CONFIG] = "custom-client-id"

    val builder = StreamsBuilder()

    val streamA = builder.stream(inputTopicA, Consumed.with(Serdes.String(), Serdes.String()))
    val streamB = builder.stream(inputTopicB, Consumed.with(Serdes.String(), Serdes.String()))
    val streamE = builder.stream(inputTopicE, Consumed.with(Serdes.String(), Serdes.String()))

    val storeSupplierC = Stores.inMemoryKeyValueStore("ktable-C-store")
    val kTableC = builder.table(
        inputTopicC,
        Materialized.`as`<String, String>(storeSupplierC)
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.String())
    )

    val storeSupplierD = Stores.inMemoryKeyValueStore("ktable-D-store")
    val kTableD = builder.table(
        inputTopicD,
        Materialized.`as`<String, String>(storeSupplierD)
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.String())
    )

    val processedStreamA = streamA
        .peek { key, value -> logRecord("Incoming streamA", key, value) }
        .filter { _, value -> value.contains("1") }
        .mapValues { value -> value.uppercase(Locale.getDefault()) }
        .peek { key, value -> logRecord("Processed streamA", key, value) }

    val processedStreamB = streamB
        .peek { key, value -> logRecord("Incoming streamB", key, value) }
        .filter { _, value -> value.contains("1") }
        .mapValues { value -> value.uppercase(Locale.getDefault()) }
        .peek { key, value -> logRecord("Processed streamB", key, value) }

    val processedKTableC = kTableC
        .filter { _, value -> value.contains("1") }
        .mapValues { value -> value.uppercase(Locale.getDefault()) }

    val joinStreamAB = processedStreamA.join(
        processedStreamB,
        { a, b -> a + b },
        JoinWindows.ofTimeDifferenceAndGrace(
            Duration.ofHours(1),
            Duration.ofMinutes(30)
        ),
        StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
    )

    val joinStreamABC = joinStreamAB.join(
        processedKTableC,
        { a, b -> a + b },
        Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
    )

    joinStreamABC
        .peek { key, value ->
            logRecord("Outgoing topic A joinStream_A_B_C record", key, value)
        }
        .to(outputTopicA, Produced.with(Serdes.String(), Serdes.String()))

    kTableD
        .mapValues { value -> value.uppercase(Locale.getDefault()) }
        .toStream()
        .peek { key, value ->
            logRecord("Outgoing topic B from kTable D", key, value)
        }
        .to(outputTopicB, Produced.with(Serdes.String(), Serdes.String()))

    val storeSupplierReduceE = Stores.inMemoryKeyValueStore("reducer-E-store")
    streamE
        .peek { key, value -> logRecord("Incoming streamE", key, value) }
        .mapValues { value -> value?.trim { it <= ' ' }?.length ?: 0 }
        .groupByKey()
        .reduce(
            { a, b -> Integer.sum(a!!, b!!) },
            Materialized.`as`<String, Int>(storeSupplierReduceE)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
        )
        .toStream()
        .mapValues { v -> "" + v }
        .peek { key, value ->
            logRecord("Outgoing topic C stream_E record", key, value)
        }
        .to(outputTopicC, Produced.with(Serdes.String(), Serdes.String()))

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
