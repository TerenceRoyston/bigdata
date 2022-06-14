package app

import bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{MyKafkaUtils, MyOffsetUtils}

/**
 * @author XuBowen
 * @date 2022/6/3 22:46
 */
object OdsBaseLogApp {
    def main(args: Array[String]): Unit = {
        // prepare realtime env
        val sparkConf = new SparkConf().setAppName("ods_base_app_log").setMaster("local[*]")
        val streamingContext = new StreamingContext(sparkConf, Seconds(5))

        val topicName = "ODS_BASE_LOG"
        val groupID = "ODS_BASE_LOG_GROUP"

        val offsets = MyOffsetUtils.readOffset(topicName, groupID)

        var kafkaDStream:InputDStream[ConsumerRecord[String,String]]=null


        if (offsets!=null && offsets.nonEmpty){
            // get kafka offset from redis
            kafkaDStream= MyKafkaUtils.getKafkaDStream(streamingContext, topicName, groupID, offsets)
        }else{
            // get kafka offset by default
            kafkaDStream= MyKafkaUtils.getKafkaDStream(streamingContext, topicName, groupID)
        }

        var offsetRanges: Array[OffsetRange] = null

        val offsetRangesDStream = kafkaDStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        // we must use map function to convert message from kafka stream because it does not implement serializable interface
        val jsonObjDStream = offsetRangesDStream.map(consumerRecord => {
            val log = consumerRecord.value()
            JSON.parseObject(log)
        })

        // get first 5 message of stream
        // jsonObjDStream.print(5)

        val DWD_PAGE_LOG_TOPIC = "DWD_PAGE_LOG_TOPIC"
        val DWD_PAGE_DISPLAY_TOPIC = "DWD_PAGE_DISPLAY_TOPIC"
        val DWD_PAGE_ACTION_TOPIC = "DWD_PAGE_ACTION_TOPIC"
        val DWD_START_LOG_TOPIC = "DWD_START_LOG_TOPIC"
        val DWD_ERROR_LOG_TOPIC = "DWD_ERROR_LOG_TOPIC"

        /**
         * split stream
         * error
         * page  which is include log,display,action
         * start
         */

        jsonObjDStream.foreachRDD(
            rdd => {
                rdd.foreach(
                    jsonObj => {
                        val errObj = jsonObj.getJSONObject("err")
                        if (errObj != null) {
                            MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, jsonObj.toJSONString)
                        } else {
                            // extract common field
                            val commonObj = jsonObj.getJSONObject("common")
                            val ar = commonObj.getString("ar")
                            val uid = commonObj.getString("uid")
                            val os = commonObj.getString("os")
                            val ch = commonObj.getString("ch")
                            val isNew = commonObj.getString("isNew")
                            val md = commonObj.getString("md")
                            val mid = commonObj.getString("mid")
                            val vc = commonObj.getString("vc")
                            val ba = commonObj.getString("ba")

                            // extract timestamp field
                            val ts = jsonObj.getLong("ts")

                            // extract page data
                            val pageObj = jsonObj.getJSONObject("page")
                            if (pageObj != null) {
                                val pageId = pageObj.getString("page_id")
                                val pageItem = pageObj.getString("item")
                                val pageItemType = pageObj.getString("item_type")
                                val duringTime = pageObj.getLong("during_time")
                                val lastPageId = pageObj.getString("last_page_id")
                                val sourceType = pageObj.getString("source_type")

                                val pageLog = PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, ts)

                                // send to topic of DWD_PAGE_LOG_TOPIC
                                MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                                // extract display data
                                val displayJsonArr = jsonObj.getJSONArray("displays")
                                if (displayJsonArr != null && displayJsonArr.size() > 0) {
                                    for (i <- 0 until displayJsonArr.size()) {
                                        val displayObj = displayJsonArr.getJSONObject(i)
                                        val displayType: String = displayObj.getString("display_type")
                                        val displayItem: String = displayObj.getString("item")
                                        val displayItemType: String = displayObj.getString("item_type")
                                        val posId: String = displayObj.getString("pos_id")
                                        val order: String = displayObj.getString("order")

                                        val pageDisplayLog =
                                            PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, displayType, displayItem, displayItemType, order, posId, ts)
                                        MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))
                                    }
                                }

                                // extract action data
                                val actionJsonArr: JSONArray = jsonObj.getJSONArray("actions")
                                if (actionJsonArr != null && actionJsonArr.size() > 0) {
                                    for (i <- 0 until actionJsonArr.size()) {
                                        val actionObj: JSONObject = actionJsonArr.getJSONObject(i)
                                        val actionId: String = actionObj.getString("action_id")
                                        val actionItem: String = actionObj.getString("item")
                                        val actionItemType: String = actionObj.getString("item_type")
                                        val actionTs: Long = actionObj.getLong("ts")

                                        val pageActionLog =
                                            PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, actionId, actionItem, actionItemType, actionTs, ts)

                                        MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))
                                    }
                                }
                            }

                            // extract start data
                            val startObj = jsonObj.getJSONObject("start")
                            if (startObj != null) {
                                val entry: String = startObj.getString("entry")
                                val loadingTime: Long = startObj.getLong("loading_time")
                                val openAdId: String = startObj.getString("open_ad_id")
                                val openAdMs: Long = startObj.getLong("open_ad_ms")
                                val openAdSkipMs: Long = startObj.getLong("open_ad_skip_ms")

                                val startLog =
                                    StartLog(mid, uid, ar, ch, isNew, md, os, vc, ba, entry, openAdId, loadingTime, openAdMs, openAdSkipMs, ts)

                                // send to topic of DWD_START_LOG_TOPIC
                                MyKafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))

                            }
                        }
                    }
                )
                MyOffsetUtils.saveOffset(topicName,groupID,offsetRanges)
            }
        )


        streamingContext.start()
        streamingContext.awaitTermination()
    }

}
