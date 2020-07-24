package com.zb.producer;

import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;

/**
 * @description: 简单消息的生产者
 * @author: zhangbing
 * @create: 2020-07-22 16:56
 **/
public class ZbSimpleProducer {

    public static void main(String[] args) throws InterruptedException {

        ZbProducer producer = new ZbProducer("my-producerGroup", "127.0.0.1:9876", false);
        //启动
        producer.start();

        //构建消息
        Message message = new Message("myTopic01", "tags-1", "send single msg".getBytes());
//        Message message = new Message("myTopic01", "tags1", "hello world consumer".getBytes());//带tags的消息
//        Message message = new Message("myTopic01", "tags2", "hello world consumer".getBytes());//带tags的消息
        producer.send(message);
        System.out.println("消息发送成功");


//        //发送批量消息
//        ArrayList<Message> messages = new ArrayList<>();
//        messages.add(new Message("myTopic01", "testTags-sendmanay1", "send batch msg1".getBytes()));
//        messages.add(new Message("myTopic01", "testTags-sendmanay2", "send batch msg2".getBytes()));
//        messages.add(new Message("myTopic01", "testTags-sendmanay3", "send batch msg3".getBytes()));
//        producer.batchSend(messages);
//        System.out.println("发送批量消息成功");
//
//        //发送异步消息
//        Message syncMessage = new Message("myTopic01", "testTags-sendsync","send sync msg".getBytes());
//        producer.syncSend(syncMessage);
//        System.out.println("发送异步消息成功");
//
//        //发送一次性消息
//        Message onewayMessage = new Message("myTopic01", "testTags-sendoneway","send oneway msg".getBytes());
//        producer.sendOneway(onewayMessage);
//        System.out.println("发送一次性消息成功");

        /**
         * 效率对比  sendOneway>sendCallBack>send批量>send单个
         */

        //消息发送完毕  关闭生产者
        Thread.currentThread().sleep(1000);
        producer.shutdown();
        System.out.println("the producer shutdown");

    }

}
