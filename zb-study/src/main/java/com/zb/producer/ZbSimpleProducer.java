package com.zb.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @description: 简单消息的生产者
 * @author: zhangbing
 * @create: 2020-07-22 16:56
 **/
public class ZbSimpleProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        DefaultMQProducer producer = new DefaultMQProducer("my-producerGroup");
        //配置nameserv的地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        //启动producer
        producer.start();

        //构建消息
        Message message = new Message("myTopic01", "hello world consumer".getBytes());
//        Message message = new Message("myTopic01", "tags1", "hello world consumer".getBytes());//带tags的消息
//        Message message = new Message("myTopic01", "tags2", "hello world consumer".getBytes());//带tags的消息
        producer.send(message);
        System.out.println("消息发送成功");

        //消息发送完毕  关闭生产者
        producer.shutdown();
        System.out.println("the producer shutdown");

    }

}
