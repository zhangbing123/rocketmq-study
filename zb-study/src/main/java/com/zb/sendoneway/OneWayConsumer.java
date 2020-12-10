package com.zb.sendoneway;

import com.zb.constans.Constans;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import static com.zb.constans.Constans.SIMPLE_TOPIC;

/**
 * @description:
 * @author: zhangbing
 * @create: 2020-12-10 18:15
 **/
public class OneWayConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ONEWAY_CONSUMER");//创建消费者
        consumer.setNamesrvAddr("47.100.15.16:9876");//配置namesrv地址
        consumer.subscribe(Constans.ONEWAY_TOPIC,"");//配置订阅的topic和tags

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.println("消费消息：" + new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;//消息确认消费成功
        });//注册消息监听

        consumer.start();
    }
}
