package com.wanggc.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
/**
 * 拦截器
 * 统计发送消息成功和发送失败消息数，并在producer关闭时打印这两个计数器
 * */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private Integer successCount = 0;
    private Integer errorCount = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        //统计失败和成功的次数
        if (exception == null){
            successCount ++;
        } else {
            errorCount ++;
        }
    }

    @Override
    public void close() {
        //保存结果
        //send success times: 1000
        //send failed times: 0
        System.out.println("send success times: " + successCount);
        System.out.println("send failed times: " + errorCount);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
