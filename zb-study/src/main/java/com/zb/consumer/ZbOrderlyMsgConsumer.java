package com.zb.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @description: 订阅顺序消息
 * @author: zhangbing
 * @create: 2020-07-24 15:47
 **/
public class ZbOrderlyMsgConsumer {

    public static void main(String[] args) throws MQClientException {
        //创建消费者  默认是集群模式
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("my-consumerGroup2");

        //配置nameserv地址
        consumer.setNamesrvAddr("127.0.0.1:9876");
        //订阅topic：myTopic01 tags= *
        consumer.subscribe("orderTopic", "*");

        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println("消费消息：" + new String(msg.getBody()) + ",thread:" + Thread.currentThread().getId());
                }
                return ConsumeOrderlyStatus.SUCCESS;//消息确认  消费成功
            }
        });

//        consumer.setConsumeThreadMax(5);  //设置最大线程数
//        consumer.setConsumeThreadMin(5);  //设置最小线程数
        consumer.start();
    }
}
