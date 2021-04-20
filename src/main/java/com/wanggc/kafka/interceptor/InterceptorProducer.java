package com.wanggc.kafka.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
/**
 * 测试拦截链
 * 1、在消息前加上时间戳
 * 2、统计消息发送成功和失败的次数。
 * */
public class InterceptorProducer {
    public static void main(String[] args) {

        //1、设置配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.52.100:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);

        //2、构建拦截链
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.wanggc.kafka.interceptor.TimeInterceptor");
        interceptors.add("com.wanggc.kafka.interceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        //创建一个生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //3、发送消息
        for (int i = 0; i < 1000; i++){
            ProducerRecord<String, String> record = new ProducerRecord<>("first", "message" + i);
            producer.send(record);
        }

        //4、一定要关闭producer，这样才会调用interceptor的close方法
        producer.close();
    }
}
