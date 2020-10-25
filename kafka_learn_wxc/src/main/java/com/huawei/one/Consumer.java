package com.huawei.one;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * Author:wxc
 * project: kafka-learn
 * ClassName: Consumer
 * Date: 2020/10/24 23:51 周六
 * yy:猥琐别浪，等我发育。
 */
public class Consumer {
    // Kafka集群地址
    private static final String brokerList = "127.0.0.1:9092";
    // 主题名称-之前已经创建
    private static final String topic = "csfq";
    // 消费组
    private static final String groupId = "group.csfq";
    public static void main(String[] args) {

        //实例化properties
        Properties properties = new Properties();
        //1. key的反序列化
        //properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //2. value的反序列化
        //properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //3. 设置集群地址
        //properties.put("bootstrap.servers", brokerList);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        //4. 指定消费组，groupId
        //properties.put("group.id", groupId);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        //指定分区，  参数一：主题，参数二：指定分区
        consumer.assign(Arrays.asList(new TopicPartition(topic,0)));

        //监听
        while (true) {
            //消息的接收
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
            //消息接收后封装到ConsumerRecord对象，然后不断的监听和打印输出
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
    }
}
