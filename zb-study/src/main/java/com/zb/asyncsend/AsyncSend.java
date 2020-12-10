package com.zb.asyncsend;

import com.zb.constans.Constans;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @description: 异步发送
 * @author: zhangbing
 * @create: 2020-12-10 18:05
 **/
public class AsyncSend {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {

        DefaultMQProducer producer = new DefaultMQProducer("async_group");
        producer.setNamesrvAddr("47.100.15.16:9876");
        producer.start();

        Message message = new Message(Constans.ASYNCSEND_TOPIC, "", "异步发送的消息".getBytes());

        producer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("消息发送成功");
            }

            @Override
            public void onException(Throwable e) {
                System.out.println("消息发送失败:"+e.getMessage());
            }
        });


        Thread.currentThread().sleep(1000);

        producer.shutdown();


    }
}
