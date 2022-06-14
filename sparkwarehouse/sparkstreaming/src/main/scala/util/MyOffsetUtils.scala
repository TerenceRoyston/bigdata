package util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * @author XuBowen
 * @date 2022/6/11 12:35
 */
object MyOffsetUtils {

    def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]) = {
        if (offsetRanges != null && offsetRanges.length > 0) {
            val offsets = new util.HashMap[String, String]()
            for (offsetRange <- offsetRanges) {
                val partition = offsetRange.partition
                val endOffset = offsetRange.untilOffset
                offsets.put(partition.toString, endOffset.toString)
            }
            println("提交offset: " + offsets)
            val jedis = MyRedisUtils.getJedisFromPool()
            val redisKey = s"$topic:$groupId"
            jedis.hset(redisKey, offsets)
            jedis.close()
        }
    }

    def readOffset(topic: String, groupId: String) = {
        val jedis = MyRedisUtils.getJedisFromPool()
        val redisKey = s"$topic:$groupId"
        val offsets = jedis.hgetAll(redisKey)
        println("读取到offset：" + offsets)
        val results = mutable.Map[TopicPartition, Long]()
        for ((partition, offset) <- offsets.asScala) {
            val tp = new TopicPartition(topic, partition.toInt)
            results.put(tp, offset.toLong)
        }
        jedis.close()
        results.toMap
    }
}
