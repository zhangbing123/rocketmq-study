package com.zb.producer;

import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * @description: 发送顺序消息
 * @author: zhangbing
 * @create: 2020-07-24 15:41
 **/
public class ZbOrderlyMsgProducer {

    public static void main(String[] args) {
        ZbProducer producer = new ZbProducer("my-producerGroup", "127.0.0.1:9876", false);
        producer.start();

        for (int i = 0; i < 5; i++) {
            Message message = new Message("orderTopic", ("hello!" + i).getBytes());

            producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get(0);
                }
            }, 0, 2000);
        }
    }
}
