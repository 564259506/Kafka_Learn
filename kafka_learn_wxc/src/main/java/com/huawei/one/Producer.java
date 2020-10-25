package com.huawei.one;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Author:wxc
 * project: kafka-learn
 * ClassName: Producer
 * Date: 2020/10/24 22:44 周六
 * yy:猥琐别浪，等我发育。
 */
public class Producer {

    private static final String brokerList = "localhost:9092";
    private static final String topic = "csfq";

    public static void main(String[] args) {
        Properties properties = new Properties();
        //1. 设置key序列化器
        //properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //2. 设置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);

        //3. 设置值序列化器
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //4. 设置集群地址
        properties.put("bootstrap.servers", brokerList);

        // 自定义拦截器使用
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, 自定义生产者拦截器.class.getName());
        //acks参数   acks = 0/1/-1    acks必须为字符串，整数就报错
        properties.put(ProducerConfig.ACKS_CONFIG,"0");

        /**
         * 作用：做消息的发送
         * 参数是上面实例化好的 properties
         */
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //封装发送消息的对象
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "kafka-demo", "熊孩子该死1228");


        try {
            //同步发送
            /*Future<RecordMetadata> send = producer.send(record);
            RecordMetadata recordMetadata = send.get();
            System.out.println("topic:" + recordMetadata.topic());
            System.out.println("partition:" + recordMetadata.partition());
            System.out.println("offset:" + recordMetadata.offset());*/

            //异步发送
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("topic:" + recordMetadata.topic());
                        System.out.println("partition:" + recordMetadata.partition());
                        System.out.println("offset:" + recordMetadata.offset());
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
