package com.zb.broadcast;

import com.zb.constans.Constans;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * @description:
 * @author: zhangbing
 * @create: 2020-12-11 10:25
 **/
public class BroadCastConsumer {

    public static void main(String[] args) throws MQClientException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("broadcast_consumer");
        consumer.setNamesrvAddr("47.100.15.16:9876");
        consumer.setMessageModel(MessageModel.BROADCASTING);//配置消费者模式为广播模式 默认是集群模式
        consumer.subscribe(Constans.BROADCAST_TOPIC, "");//订阅topic
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {

            for (MessageExt msg : msgs) {
                System.out.println("消息消费成功:" + new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();

    }
}
