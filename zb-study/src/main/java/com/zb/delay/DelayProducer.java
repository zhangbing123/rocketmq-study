package com.zb.delay;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import static com.zb.constans.Constans.DELAY_TOPIC;

/**
 * @description:
 * @author: zhangbing
 * @create: 2020-12-11 16:43
 **/
public class DelayProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("delay_group");
        producer.setNamesrvAddr("47.100.15.16:9876");
        producer.setRetryTimesWhenSendFailed(3);//配置发送失败时候的重试次数  默认是重试3次 可以配置
        producer.start();

        //构建消息
        Message message = new Message(DELAY_TOPIC, "tags-2", "send single msg:".getBytes());
        message.setDelayTimeLevel(3);//配置延迟级别 1s,5s,10s,30s,1m,2m,3m,4m,5m,6m,7m,8m,9m,10m,1h,2h
        SendResult send = producer.send(message);
        System.out.println("消息发送成功:" + send);

        //消息发送完毕  关闭生产者
        producer.shutdown();
    }
}
