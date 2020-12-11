package com.zb.broadcast;

import com.zb.constans.Constans;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @description: 广播模式
 * @author: zhangbing
 * @create: 2020-12-11 10:22
 **/
public class BroadCastProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        DefaultMQProducer producer = new DefaultMQProducer("broadcast_group");
        producer.setNamesrvAddr("47.100.15.16:9876");
        producer.start();

        Message message = new Message(Constans.BROADCAST_TOPIC, "测试广播消息".getBytes());

        producer.send(message);

        producer.shutdown();

        System.out.println("消息发送成功");

    }
}
