package com.xubowen.consume;


import kafka.api.FetchRequestBuilder;
import kafka.api.FetchRequest;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author XuBowen
 * @date 2021/11/20 19:17
 */
public class LowConsumer {
    public static void main(String[] args) throws UnsupportedEncodingException {
        BrokerEndPoint leader =null;
        // 创建简单消费者
        String host="CentOS-02";
        int port=9092;

        // 获取分区leader
        SimpleConsumer metaConsumer = new SimpleConsumer(host, port, 500, 10 * 1024, "metadata");

        // 获取元数据信息
        TopicMetadataRequest request = new TopicMetadataRequest(Arrays.asList("xubowen"));
        TopicMetadataResponse response = metaConsumer.send(request);
        // 双层for循环如何直接跳出？ 加上标记
        leaderLabel:
        for (TopicMetadata topicMetadata : response.topicsMetadata()) {
            if (topicMetadata.topic().equals("xubowen")) {
                for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
                    int partid = partitionMetadata.partitionId();
                    if (partid == 1){
                       leader = partitionMetadata.leader();
                       break leaderLabel;
                    }
                }
            }
        }

        if (leader == null){
            System.out.println("分区信息不正确");
            return;
        }


        SimpleConsumer consumer = new SimpleConsumer(leader.host(),leader.port(),500,10*1024,"accessLeader");
        // 消费者抓取数据
        FetchRequest req = new FetchRequestBuilder().addFetch("xubowen",2,1,10*1024).build();
        FetchResponse resp = consumer.fetch(req);
        ByteBufferMessageSet messageSet = resp.messageSet("xubowen", 2);
        for (MessageAndOffset messageAndOffset : messageSet) {
            ByteBuffer buffer = messageAndOffset.message().payload();
            byte[] bs = new byte[buffer.limit()];
            buffer.get(bs);
            String value = new String(bs, "utf-8");
            System.out.println(value);
        }

        /**
         * 乱码处理方案
         * 先将乱码按照原本的编码方式恢复成字节码
         * 再将字节码按照目标编码格式转换
         */
        String s="???";
        byte[] bytes = s.getBytes("ISO8859-1");
        String resStr = new String(bytes, "utf-8");


    }

}
