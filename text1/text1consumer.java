package text1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class text1consumer {
    public static void main(String[] args) {

        //1.使用Properties定义配置属性
        Properties props = new Properties();

        //设置消费者Broker连接地址
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.146.133:9092,192.168.146.134:9092,192.168.146.135:9092");

        //设置反序列化key的程序类,与生产者对应。
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //设置反序列化value的程序类，与生产者对应
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

        //设置消费者组ID，即组名称，值可自定义。组名称相同的消费者进程属于同一个消费者组
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"groupid-1");

        //2.定义消费者对象
        Consumer<String,Integer> consumer = new KafkaConsumer<String, Integer>(props);

        //3.设置消费者读取的主题名称，值可定义。组名称相同的消费者进程属于同一个消费者组
        consumer.subscribe(Arrays.asList("topictest"));

        //4.不停的读取消息
        while(true){

            //不停的读取消息
            ConsumerRecords<String,Integer> records = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, Integer> record : records){

                //打印消息关键信息
                System.out.println("Key:" + record.key()+", value:" + record.value() + ", partition:"+record.partition()
                +", offset:" + record.offset());
            }
        }
    }
}
