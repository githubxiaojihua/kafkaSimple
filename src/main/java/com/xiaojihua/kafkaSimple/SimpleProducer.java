package com.xiaojihua.kafkaSimple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * kafka简单生产者。
 * 从main函数接收主题名称，并发送消息至kafka集群
 * 本程序对应的kafka服务端安装的是0.9.0.0
 */
public class SimpleProducer {

    public static void main(String[] args) {

        // 检查输入参数
        if(args.length == 0){
            System.out.println("Enter topic name!");
            return;
        }

        // 获取topic名称
        String topicName = args[0].toString();

        // 创建配置对象盛放生产者配置数据
        Properties props = new Properties();
        // 指定代理
        props.put("bootstrap.servers","192.168.33.129:9092");
        // 指定请求标准，all代表满足所有请求标准
        props.put("acks","all");
        // 如果请求失败，生产者可自动重试
        props.put("retries",0);
        // 缓冲区大小
        props.put("batch.size",16384);
        //
        props.put("linger.ms",1);
        // 控制生产者可用于缓冲的存储器的总量
        props.put("buf  fer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for(int i = 0; i < 10; i++)
            // ProducerRecord()
            producer.send(new ProducerRecord<String, String>(topicName,Integer.toString(i), Integer.toString(i)));
        System.out.println("Message sent successfully");
        producer.close();
    }
}
