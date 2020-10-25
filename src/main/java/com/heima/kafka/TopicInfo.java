package com.heima.kafka;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author dayuan
 * @Date 2019/7/11 11:28
 */
public class TopicInfo {

    public static final String brokerList = "localhost:9092";
    public static final String topic = "heima";
    public static final String groupId = "group.heima";
    private static AtomicBoolean running = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();
        System.out.println(JSONObject.toJSONString(stringListMap));
        //if(MapUtils.isNotEmpty(stringListMap)){
        //    Set<String> keys = stringListMap.keySet();
        //    for(String key:keys){
        //        List<PartitionInfo> partitionInfos = stringListMap.get(key);
        //        if(!partitionInfos.isEmpty()){
        //            for(PartitionInfo partitionInfo:partitionInfos){
        //                System.out.println("topic:"+partitionInfo.topic());
        //                System.out.println("partition:"+partitionInfo.partition());
        //            }
        //        }
        //    }
        //}

        TopicPartition tp = new TopicPartition(topic,0);
        long posititon = consumer.position(tp);
        System.out.println("the offset of the next record is " + posititon);
        System.out.println();
    }
}
