package com.zb.order;

import com.zb.constans.Constans;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.CountDownLatch;

/**
 * @description: 顺序消息发送
 * @author: zhangbing
 * @create: 2020-12-11 15:20
 **/
public class OrderlyProducer {


    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("order_group");
        producer.setNamesrvAddr("47.100.15.16:9876");
        producer.start();

        CountDownLatch countDownLatch = new CountDownLatch(30);

        for (int i = 1; i < 4; i++) {//发送一个topic下的3个tags

            for (int j = 0; j < 10; j++) {//针对一个tags  发送10条消息

                Message message = new Message(Constans.ORDERLY_TOPIC, "tags-" + i, ("tags-" + i + ",顺序消息:" + +j).getBytes());

                producer.send(message, (mqs, msg, arg) -> {
                    countDownLatch.countDown();
                    int k = (int) arg;
                    return mqs.get(k % mqs.size());//选择第一个队列
                }, i);
            }
        }

        countDownLatch.await();
        System.out.println("消息发送完成");
        producer.shutdown();
    }


}
