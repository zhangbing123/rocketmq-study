package com.zb.transaction;

import com.zb.constans.Constans;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @description:
 * @author: zhangbing
 * @create: 2020-12-11 14:14
 **/
public class TransactionConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("transaction_consumer");
        consumer.setNamesrvAddr("47.100.15.16:9876");
        consumer.subscribe(Constans.TRANSACTION_TOPIC, "tags-1||tags-2||tags-3");

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {

            for (MessageExt msg : msgs) {

                System.out.println("消费者接受到消息:" + new String(msg.getBody()));

            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();


    }
}
