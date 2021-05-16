package text1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class text1producer {
    public static void main(String[] args) {

        //1.使用Properties定义配置属性
        Properties props = new Properties();

        //设置生产者Broker服务器连接地址192.168.146.133
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.146.133:9092,192.168.146.134:9092,192.168.146.135:9092");

        //设置序列化key程序类
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //设置序列化value程序类
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

        //2.定义消息生产者对象，依靠此对象可以进行消息的传递
        Producer<String,Integer> producer = new KafkaProducer<String, Integer>(props);

        //3.循环发送10条消息
        for (int i = 10; i < 20; i++) {

            /**
             * 发送消息，此方式只负责发送消息，不关心消息是否发送成功
             */
            producer.send(new ProducerRecord<String,Integer>("topictest","hello kafka " + i,i));
        }

        //4.关闭生产者，释放资源
        producer.close();

    }
}
