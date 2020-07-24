package com.zb.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @description: 简单消息消费者
 * @author: zhangbing
 * @create: 2020-07-22 16:56
 **/
public class ZbSimpleConsumer {

    public static void main(String[] args) throws MQClientException {
        //创建消费者  默认是集群模式
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("my-consumerGroup");

        //配置nameserv地址
        consumer.setNamesrvAddr("127.0.0.1:9876");
        //订阅topic：myTopic01 tags= *
        consumer.subscribe("transactionTopic", "*");

        //可以配置广播模式：消费组中的所有消费者都可以消费的到
//        consumer.setMessageModel(MessageModel.BROADCASTING);

        //注册监听器 进行消息的监听以及接收
        consumer.registerMessageListener(new MessageListenerConcurrently() {//并发消息
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

                for (MessageExt msg : msgs) {
                    System.out.println("消费消息：" + new String(msg.getBody()));
                }


                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;//消息确认  消费成功
            }
        });

//        consumer.registerMessageListener(new MessageListenerOrderly() {//顺序消息
//            @Override
//            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
//                return null;
//            }
//        });

        //启动消费者
        consumer.start();

    }
}
