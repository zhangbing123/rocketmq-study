package com.zb.batchsend;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

import static com.zb.constans.Constans.BATCHSEND_TOPIC;

/**
 * @description:
 * @author: zhangbing
 * @create: 2020-12-10 16:47
 **/
public class BatchProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("BATCHSEND");
        producer.setNamesrvAddr("47.100.15.16:9876");
        producer.start();

        /**
         * 批量发送的时候  要求所有消息的topic一样
         */
        List<Message> messageList = new ArrayList<>(3);
        messageList.add(new Message(BATCHSEND_TOPIC, "tag-1", "send batch msg1".getBytes()));
        messageList.add(new Message(BATCHSEND_TOPIC, "tag-2", "send batch msg2".getBytes()));
        messageList.add(new Message(BATCHSEND_TOPIC, "tag-3", "send batch msg3".getBytes()));

        SendResult sendResult = producer.send(messageList);
        System.out.println("消息发送成功:" + sendResult);
        producer.shutdown();


    }
}
