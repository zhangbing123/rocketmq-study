package com.zb.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @description: 消息生产者
 * @author: zhangbing
 * @create: 2020-07-22 17:31
 **/
public class ZbProducer {

    private String producerGroup;

    private String nameSvr;

    private MQProducer mqProducer;

    public ZbProducer(String producerGroup, String nameSvr, boolean isTransaction) {
        this.producerGroup = producerGroup;
        this.nameSvr = nameSvr;
        if (isTransaction) {
            mqProducer = new TransactionMQProducer(producerGroup);
        } else {
            mqProducer = new DefaultMQProducer(producerGroup);
        }
        //配置nameserv的地址
        ((DefaultMQProducer) mqProducer).setNamesrvAddr(nameSvr);
        try {
            //启动producer
            mqProducer.start();
        } catch (MQClientException e) {

        }
    }


    SendResult send(Message msg) {
        try {
            return mqProducer.send(msg);
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    void shutdown() {
        mqProducer.shutdown();
    }
}
