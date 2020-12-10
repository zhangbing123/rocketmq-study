package com.zb.asyncsend;

import com.zb.constans.Constans;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @description:
 * @author: zhangbing
 * @create: 2020-12-10 18:08
 **/
public class AsyncConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("async_consumer");
        consumer.setNamesrvAddr("47.100.15.16:9876");
        consumer.subscribe(Constans.ASYNCSEND_TOPIC, "");
        //异步发送
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {

            for (MessageExt msg : msgs) {
                System.out.println("消费者接受消息:" + new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();//启动

    }
}
