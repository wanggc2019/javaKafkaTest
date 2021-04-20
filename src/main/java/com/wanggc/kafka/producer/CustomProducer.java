package com.wanggc.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 生产者
 * */
public class CustomProducer {

    public static void main(String[] args) {

        //1、设置配置信息
        Properties properties = new Properties();

        //kafka集群broker的ip和端口
        // 默认端口是9092，多写几个，防止down了，也没必要都写上
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.52.100:9092, 192.168.52.100:9093, 192.168.52.100:9094");

        //key序列化(java对象-->字节序列)
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //value序列化
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //等待所有副本节点的应答
        properties.put(ProducerConfig.ACKS_CONFIG,"all");

        //客户端消息缓存，默认33554432bytes=32M
        //客户端发送的消息先放入客户端的缓存，然后把缓存中的消息聚合成一个个batch，然后再把这些batch打包成一个request发送到broker（sender线程做）
        //如果设置的太小，客户端缓存很快就被写满，一旦写满就会阻塞用户进程，消息写入阻塞。
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        //batch的小大，默认16384bytes=16k
        //一个batch凑够了16k的数据就可以发送了，调大batch，可以允许更多的数据缓存在batch，一次request发送的数据就变多了，吞吐量或会有所提升。
        //如果设置的太大，数据缓存在batch迟迟不发，将会增加延迟
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        //请求延时，默认0ms
        //一个Batch被创建之后，最多过多久，不管这个Batch有没有写满，都必须发送出去了
        //根据业务设置，一个batch多长时间会被填满，设置太小会导致batch始终填不满。
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        //request的大小默认1048576=1M
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,1048576);
        //请求失败重试,默认5次
        properties.put(ProducerConfig.RETRIES_CONFIG,5);
        //失败重试的间隔是多少，默认100ms
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

        //使用自定义的分区
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.wanggc.kafka.producer.CustomPartitioner");

        //2、创建一个生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //3、调用send方法
        //发送1000条消息
        for (int i = 0; i < 100; i++){
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first", Integer.toString(i), "message-" + i);
            producer.send(record);
        }

        //4、关闭生产者
        producer.close();

    }
}
