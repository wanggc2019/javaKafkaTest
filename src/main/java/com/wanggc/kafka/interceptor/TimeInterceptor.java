package com.wanggc.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
/**
 * 时间戳拦截器
 * */
public class TimeInterceptor implements ProducerInterceptor<String, String> {
    //创建一个新的record，把时间戳写入消息体的最前部
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        //1594615744767,message870
        //1594615744767,message873
        //1594615744767,message876
        return new ProducerRecord(record.topic(), record.partition(), record.timestamp(), record.key(), System.currentTimeMillis() + "," + record.value().toString());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
