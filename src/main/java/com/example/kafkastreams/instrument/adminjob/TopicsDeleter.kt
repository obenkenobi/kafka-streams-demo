package com.example.kafkastreams.instrument.adminjob

import com.example.kafkastreams.loadProperties
import org.apache.kafka.clients.admin.Admin
import java.io.IOException

fun main() {
    runDeleter()
}
@Throws(IOException::class)
fun runDeleter() {
    val properties = loadProperties()
    Admin.create(properties).use { adminClient ->
        val inputTopicA = properties.getProperty("instrument_a.input.topic")
        val inputTopicB = properties.getProperty("instrument_b.input.topic")
        val inputTopicC = properties.getProperty("instrument_c.input.topic")
        val inputTopicD = properties.getProperty("instrument_d.input.topic")
        val inputTopicE = properties.getProperty("instrument_e.input.topic")
        val outputTopicA = properties.getProperty("instrument_a.output.topic")
        val outputTopicB = properties.getProperty("instrument_b.output.topic")
        val outputTopicC = properties.getProperty("instrument_c.output.topic")
        val topics = listOf(
            inputTopicA, inputTopicB, inputTopicD,
            inputTopicC, inputTopicE, outputTopicA, outputTopicB, outputTopicC
        )
        adminClient.deleteTopics(topics)
    }
}