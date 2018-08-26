package com.xiaojihua.kafkaSimple;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * kafka简单消费者实例
 *
 * 消费者组：当多个消费者的group.id相同的时候那么他们就是属于同一个
 * 消费者组。同一个消费者组中消费主题中位于不同分区的消息。每一个分区
 * 只能分配给消费者组中的一个消费者。
 * 如果创建主题的时候只制定了一个分区那么，将只有一个消费者消费所有消息。
 * 如果主题有多个分区，那么消息将分布到不同的分区上，那么消费者组中的
 * 所有消费者分摊所有消息。
 *
 * 当本例单个使用的时候是单个消费者。
 * 如果本实例代码作为另外一个新类运行的话，那么根本类就是同属于test组的消费者。
 *
 * 消费者组通过指定group.id来确定。
 */
public class SimpleConsumer {

    public static void main(String[] args) {

        if(args.length == 0){
            System.out.println("Enter topic name");
            return;
        }

        String topicName = args[0].toString();
        Properties props = new Properties();
        // 制定要连接的代理
        props.put("bootstrap.servers","192.168.33.129:9092");
        // 将单个消费者分配给组
        props.put("group.id","test");
        // 如果值为true,则为偏移启动自动落实，否则不提交
        props.put("enable.auto.commit","true");
        // 更新偏移量的频率
        props.put("auto.commit.interval.ms","1000");
        // 超时时间
        props.put("session.timeout.ms","30000");
        // 键值序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建kafkaconsumer实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // 指定主题
        consumer.subscribe(Arrays.asList(topicName));

        System.out.println("Subscribed to topic" + topicName);

        while(true){
            // 拉取主题中的消息
            ConsumerRecords<String,String> records = consumer.poll(1000);
            for(ConsumerRecord<String,String> record : records){
                System.out.printf("offset=%d,key=%s,value=%s\n",record.offset(),record.key(),record.value());
            }
        }



    }
}
