package com.zb.simple;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import static com.zb.constans.Constans.SIMPLE_TOPIC;

/**
 * @description: 生产者
 * @author: zhangbing
 * @create: 2020-12-10 15:23
 **/
public class Producer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        DefaultMQProducer producer = new DefaultMQProducer("SIMPLE");
        producer.setNamesrvAddr("47.100.15.16:9876");
        producer.setRetryTimesWhenSendFailed(3);//配置发送失败时候的重试次数  默认是重试3次 可以配置
        producer.start();

        //构建消息
//        for (int i = 0; i < 10; i++) {
//            Message message = new Message(SIMPLE_TOPIC, "tags-2", ("send single msg:" + i).getBytes());
//            SendResult send = producer.send(message);
//            System.out.println("消息发送成功:" + send);
//        }

        Message message = new Message(SIMPLE_TOPIC, "tags-2", ("send single msg:" + 2).getBytes());
        SendResult send = producer.send(message);
        System.out.println("消息发送成功:" + send);


        //消息发送完毕  关闭生产者
        producer.shutdown();
    }
}
