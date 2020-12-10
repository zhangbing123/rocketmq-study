package com.zb.simple;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import static com.zb.constans.Constans.SIMPLE_TOPIC;

/**
 * @description:
 * @author: zhangbing
 * @create: 2020-12-10 15:29
 **/
public class Consumer {

    /**
     * 一个消费者组中的消费者一定要订阅相同的topic下的tags
     *
     * 要是一个消费者组中的 不同消费者 订阅同一个topic下的不同tags 会出现消费不到消息的情况
     * @param args
     * @throws MQClientException
     */
    public static void main(String[] args) throws MQClientException {

        String postfix = "2";

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("SIMPLE" + postfix);//创建消费者
        consumer.setNamesrvAddr("47.100.15.16:9876");//配置namesrv地址
        consumer.subscribe(SIMPLE_TOPIC, "tags-" + postfix);//配置订阅的topic和tags

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.println("消费消息：" + new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;//消息确认消费成功
        });//注册消息监听

        consumer.start();

    }
}