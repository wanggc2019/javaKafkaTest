package com.wgc.javaKafkaTest;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class KafkaProducerClient {

    //异步发送消息得回调函数
    private static class DempProducerCallback implements Callback{
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null){
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        /**
         * 1、kafka jass登陆文件
         */
        System.setProperty("java.security.auth.login.config", "E:\\IdeaProjects\\javaKafkaTest\\src\\main\\resources\\jaas.conf");
        //Kerberos客户端配置文件
        System.setProperty("java.security.krb5.conf", "E:\\IdeaProjects\\javaKafkaTest\\src\\main\\resources\\krb5.conf");

        /**
         * 2、配置kafka参数属性
         */
        Properties properties = new Properties();

        //指定kafka broker得地址清单，不必全部指定，生产这会从指定得broker里找到其他得broker清单，建议至少配置2个，一旦一个宕机，
        //另一个可以继续工作
        properties.put("bootstrap.servers","134.64.14.230:9092,134.64.14.231:9092,134.64.14.232:9092");


        //当生产者将ack设置为“全部”（或“-1”）时，min.insync.replicas指定必须确认写入被认为成功的最小副本数。
        //如果这个最小值不能满足，那么生产者将会引发一个异常（NotEnoughReplicas或NotEnoughReplicasAfterAppend）。
        //当一起使用时，min.insync.replicas和acks允许您执行更大的耐久性保证。
        //一个典型的情况是创建一个复制因子为3的主题，将min.insync.replicas设置为2，并使用“全部”选项来产生。
        //这将确保生产者如果大多数副本没有收到写入引发异常。
        //acks指定了必须要多少个分区副本收到消息
        //acks=0，生产者在成功写入消息之前不会等待任何来自服务器得响应，可能会丢消息，支持最大得速度发消息，高吞吐量
        //acks=1，只要集群得首领节点收到消息，生产者就会收到一个来自服务器得成功响应
        //acks=all,只有当参与复制得所有节点都收到消息时，生产者蔡会收到一个来自服务器得成功响应
        properties.put("acks","all");

        //设置一个大于零的值,将导致客户端重新发送任何失败的记录
        //重试机制
        //properties.put("retries",0);
        properties.put("retries",3);

        //只要有多个记录被发送到同一个分区，生产者就会尝试将记录一起分成更少的请求。
        //这有助于客户端和服务器的性能。该配置以字节为单位控制默认的批量大小。
        //指定一个批次使用得内存大小，字节，并不一定等到批次慢了在发送
        properties.put("batch.size",16384);

        //在某些情况下，即使在中等负载下，客户端也可能希望减少请求的数量。
        //这个设置通过添加少量的人工延迟来实现这一点，即不是立即发出一个记录，
        //而是等待达到给定延迟的记录，以允许发送其他记录，以便发送可以一起批量发送
        //增加延迟，提高吞吐量
        properties.put("linger.ms",1);

        //生产者可用于缓冲等待发送到服务器的记录的总字节数。
        //如果记录的发送速度比发送给服务器的速度快，那么生产者将会阻塞，max.block.ms之后会抛出异常。
        //这个设置应该大致对应于生产者将使用的总内存，但不是硬性限制，
        //因为不是所有生产者使用的内存都用于缓冲。
        //一些额外的内存将被用于压缩（如果压缩被启用）以及用于维护正在进行的请求。
        properties.put("buffer.memory",33554432);

        //默认消息发送时不压缩
        //压缩格式：snappy，占用较少cpu，可观得压缩比，关注性能和网络带宽
        //gzip，较高压缩比，占用cpu多，适用网络带宽有限
        //lz4
        properties.put("compression.type","snappy");

        //
        properties.put("max.in.flight.requests.per.connection",1);

        //key的序列化类型。
        // broker希望收到得消息键值对为字节数组，生产使用系列化得类将java对象转化为字节数组，这是必须设置得。
        //无论发不发送key和value
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //value的序列化
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        /**
         * 3、kafka kerberos
         */
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.kerberos.service.name", "kafka");
        properties.put("sasl.mechanism", "GSSAPI");

        /**
         * 4、创建KafkaProducer
         */
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);
        //发送消息得方式：
        //1、发送并忘记，消息发送到服务器，不关心是否到达，可能会丢失一些数据
        //2、同步发送，send()发送消息，返回Futrue对象，调用get方法等待，可知道是否发送成功
        //3、异步发送，调用send方法，指定回调函数，服务器在返回响应时调用该函数
/*        for (int i=0;i<100;i++){
            //发送数据
            kafkaProducer.send(new ProducerRecord<>("Test","key"+i,"value"+i));
        }*/

        //最简单得发送消息，发送并忘记
/*        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("Test","wgc","Chinese");
        try {
            kafkaProducer.send(producerRecord);
        } catch (Exception e){
            e.printStackTrace();
        }*/

        //同步发送
/*        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("Test","wgc","Chinese_get");
        try {
            kafkaProducer.send(producerRecord).get();
        } catch (Exception e){
            e.printStackTrace();
        }
 */
        //异步发送消息
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("Test","wgc","Chinese_callback");
        //DempProducerCallback dempProducerCallback = new DempProducerCallback();
        kafkaProducer.send(producerRecord,new DempProducerCallback());

        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
