package com.wanggc.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * 测试consumer API
 * */
public class CustomConsumer {
    public static void main(String[] args) {

        Properties properties = new Properties();
        //建立连接
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.52.100:9092");
        //key,value的反序列化StringDeserializer 注意要和producer区别，producer是序列化StringSerializer
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //自动提交offset，可以不用显示设置，默认为true
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        //自动提交的频率，可以不用显示设置，默认为5000ms
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);

        //指定消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"wgc");

        // 1、创建一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //订阅消费的主题
        consumer.subscribe(Arrays.asList("first"));
        //当消费者组内加入一个新的消费者时，需要重新回收所有分区并重启分配
/*        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("===>回收的分区.");
                for (TopicPartition partition : partitions) {
                    System.out.println("partitions = " + partition);
                }

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("===>重新得到的分区.");
                for (TopicPartition partition : partitions) {
                    System.out.println("partition = " + partition);
                }
            }
        });*/

        //2、调用poll（轮询）
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);// 100ms
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic = " + record.topic() + ", offset = " + record.offset() + ", value = " + record.value());

                //同步提交
                consumer.commitSync();
            }

        }
    }
}
