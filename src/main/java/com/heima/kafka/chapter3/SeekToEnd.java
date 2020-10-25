package com.heima.kafka.chapter3;

import com.heima.kafka.ConsumerClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * 指定位移开始消费  末尾
 */
public class SeekToEnd extends ConsumerClientConfig {


    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        // 指定从分区末尾开始消费
        Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        for (TopicPartition tp : assignment) {
            //consumer.seek(tp, offsets.get(tp));
            consumer.seek(tp, offsets.get(tp) + 1);
        }
        System.out.println(assignment);
        System.out.println(offsets);

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
            //consume the record.
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset() + ":" + record.value());
            }
        }

    }
}
