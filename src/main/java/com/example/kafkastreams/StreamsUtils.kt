package com.example.kafkastreams

import org.apache.kafka.clients.admin.NewTopic
import java.io.FileInputStream
import java.io.IOException
import java.util.*

object StreamsUtils {
    private const val PROPERTIES_FILE_PATH = "src/main/resources/streams.properties"
    private const val REPLICATION_FACTOR: Short = 3
    private const val PARTITIONS = 6

    @JvmStatic
    @Throws(IOException::class)
    fun loadProperties(): Properties {
        val properties = Properties()
        FileInputStream(PROPERTIES_FILE_PATH).use { fis ->
            properties.load(fis)
            return properties
        }
    }

    @JvmStatic
    fun createTopic(topicName: String?): NewTopic {
        return NewTopic(topicName, PARTITIONS, REPLICATION_FACTOR)
    }
}