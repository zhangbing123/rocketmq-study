package com.zb.pullmode;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import static com.zb.constans.Constans.PULL_TOPIC;

/**
 * @description: 生产者
 * @author: zhangbing
 * @create: 2020-12-10 15:23
 **/
public class Producer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        DefaultMQProducer producer = new DefaultMQProducer("PULL_GROUP");
        producer.setNamesrvAddr("47.100.15.16:9876");
        producer.setRetryTimesWhenSendFailed(3);//配置发送失败时候的重试次数  默认是重试3次 可以配置
        producer.start();

        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                //构建消息
                for (int i1 = 0; i1 < 1000; i1++) {
                    Message message = new Message(PULL_TOPIC, "tags-2", (Thread.currentThread().getId() + "send single msg:" + i1).getBytes());
                    SendResult send = null;
                    try {
                        send = producer.send(message);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("消息发送成功:" + send);
                }
            }).start();
        }

        //消息发送完毕  关闭生产者
//        producer.shutdown();
    }
}
