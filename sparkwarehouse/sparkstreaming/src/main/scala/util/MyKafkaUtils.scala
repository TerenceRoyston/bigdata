package util

import java.util.Properties

import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 * @author XuBowen
 * @date 2022/6/3 17:38
 */
object MyKafkaUtils {

    val producer: KafkaProducer[String, String] = createProducer()


    // consumer config
    val consumerConfigs = mutable.Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hdp1:6667,hdp2:6667,hdp3:6667",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
    )


    // consume data  default offset
    def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String) = {
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        val kafkaDStream = KafkaUtils.createDirectStream(
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs)
        )

        kafkaDStream
    }

    // consume data  offset from redis
    def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String, offsets: Map[TopicPartition, Long]) = {
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        val kafkaDStream = KafkaUtils.createDirectStream(
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs, offsets)
        )

        kafkaDStream
    }

    // produce data
    def createProducer() = {
        // producer config
        val prop = new Properties()
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp1:6667,hdp2:6667,hdp3:6667")
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        prop.put(ProducerConfig.ACKS_CONFIG, "all")

        new KafkaProducer[String, String](prop)
    }

    // send method
    def send(topic: String, msg: String) = {
        producer.send(new ProducerRecord[String, String](topic, msg))
    }

    def send(topic: String, key: String, msg: String) = {
        producer.send(new ProducerRecord[String, String](topic, key, msg))
    }

    // close method
    def close = {
        if (producer != null) {
            producer.close()
        }
    }
}
