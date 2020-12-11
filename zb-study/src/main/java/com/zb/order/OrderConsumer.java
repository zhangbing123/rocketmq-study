package com.zb.order;

import com.zb.constans.Constans;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @description:
 * @author: zhangbing
 * @create: 2020-12-11 15:28
 **/
public class OrderConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("orderly_consumer");
        consumer.setNamesrvAddr("47.100.15.16:9876");
        consumer.subscribe(Constans.ORDERLY_TOPIC, "tags-1||tags-2||tags-3");
        consumer.setConsumeThreadMax(1);//设置消费的最大线程数1
        consumer.setConsumeThreadMin(1);//设置消费的最小线程数1
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {

            System.out.println(Thread.currentThread().getName() + "开始消费消息....");
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (MessageExt msg : msgs) {
                System.out.println("消费消息：" + new String(msg.getBody()));
            }
            return ConsumeOrderlyStatus.SUCCESS;//消息确认消费成功
        });

        consumer.start();
    }
}
