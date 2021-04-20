package com.wgc.javaKafkaTest;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



//消费者群组
//添加了新分区-->分区重分配-->分区所有权从一个消费者转移到另一个消费者
public class KafkaConsumerClient {
    public static void main(String[] args) {
/*        String broker_server =
                "134.64.14.230:9092," +
                "134.64.14.231:9092," +
                "134.64.14.232:9092," +
                "134.64.14.233:9092," +
                "134.64.14.234:9092," +
                "134.64.14.235:9092, " +
                "134.64.14.236:9092, " +
                "134.64.14.237:9092," +
                "134.64.14.238:9092," +
                "134.64.14.239:9092";
*/
        /**
         * kerberos验证
         */
        System.setProperty("java.security.auth.login.config", "E:\\IdeaProjects\\javaKafkaTest\\src\\main\\resources\\jaas.conf");
        System.setProperty("java.security.krb5.conf", "E:\\IdeaProjects\\javaKafkaTest\\src\\main\\resources\\krb5.conf");
        /**
         * 配置kafka属性文件
         */
        Properties properties = new Properties();

        //定义kakfa 服务的地址，不需要将所有broker指定上
        properties.put("bootstrap.servers", "134.64.14.230:9092,134.64.14.231:9092,134.64.14.232:9092");

        //指定消费者
        properties.put("group.id", "wgc");

        //指定消费者是否自动提交偏移量，默认true
        //如果true,consumer定期地往zookeeper写入每个分区的offset
        properties.put("enable.auto.commit", "true");
        //控制提交得频率
        // 往zookeeper上写offset的频率
        properties.put("auto.commit.interval.ms", 1000); //1s

        //指定消费者从服务器获取记录得最小字节
        //即告诉kafka等到有足够数据时才将他返回给消费者
        properties.put("fetch.min.bytes", 1024 * 1024 * 1024); //1MB

        //指定broker得等待时间
        //这个值配合上面得值使用，要么在指定时间内数据达到1M发送给消费者，要么在指定时间未达到1M而超时发送。
        properties.put("fetch.max.wait.ms", 100); //100ms

        //服务器从每个分区里返回给消费者得最大字节数，默认1M
        properties.put("max.partition.fetch.bytes", 2048000); //2M

        // key的反序列化类型，字节-->对象
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的反序列化类型，字节-->对象
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //kerberos
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.kerberos.service.name", "kafka");
        properties.put("sasl.mechanism", "GSSAPI");
        /**
         * 创建consumer
         */
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        /**
         * 订阅topic
         */
        //这里的topic可以是多个
        kafkaConsumer.subscribe(Arrays.asList("Test", "KHST5GTURNON"));

        /**
         * 轮询
         */
        //提交和偏移量
        //1、自动提交， properties.put("enable.auto.commit", "true");
        //2、提交当前偏移量，properties.put("enable.auto.commit", "false");
        //3、异步提交
        //4、同步和异常组合提交
        //这是一个无限循环，消费者实际是一个长期运行得应用程序
        Logger logger = LoggerFactory.getLogger(KafkaConsumerClient.class);
        try {
            while (true) {
                //消费者必须持续对kafka进行轮询
                //超时时间，控制poll方法得阻塞时间
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
                //poll方法返回一个记录列表
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.printf("topic = %s, partition = %s, offset = %d, key = %s, value = %s%n",
                            record.topic(), record.offset(), record.key(), record.value());
                }
                //提交当前偏移量
/*                try {
                    kafkaConsumer.commitSync();
                } catch (CommitFailedException e){
                    logger.error("commit failed!",e);
                }*/
                //异步提交
                kafkaConsumer.commitAsync();//提交最后一个偏移量然后继续做其他事情
            }
        } catch (Exception e) {
            logger.error("Unexpected error!", e);
        } finally {
            try {
                kafkaConsumer.commitSync();
            } finally {
                //同步异步结合，如果一切正常用commitAsync提交，如果是直接关闭消费者则commitSync会一直重试，直到成功或者发生
                //无法恢复的错误
                kafkaConsumer.close();
            }
        }
    }
}
