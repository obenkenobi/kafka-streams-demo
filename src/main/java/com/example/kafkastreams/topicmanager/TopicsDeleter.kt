package com.example.kafkastreams.topicmanager

import com.example.kafkastreams.loadProperties
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.ListTopicsOptions
import java.io.IOException

fun main() {
    runDeleter()
}
@Throws(IOException::class)
fun runDeleter() {
    val properties = loadProperties()
    Admin.create(properties).use { adminClient ->
        val listTopicOpts = ListTopicsOptions().listInternal(true)
        val topics = adminClient.listTopics(listTopicOpts).names().get()
        adminClient.deleteTopics(topics)
    }
}