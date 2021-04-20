package com.wanggc.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SyncProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.52.100:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1000);

        //1、创建一个生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //2、调用send方法
        //send方法返回Future对象 ，future.get()
        for (int i=0;i<1000;i++){
            RecordMetadata meta = producer.send(new ProducerRecord<String, String>("first", i + "", "message-" + i)).get();
            System.out.println("offset: " + meta.offset());
        }

        //3、关闭生产者
        producer.close();

    }
}
