package com.zb.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 * @description: 消息生产者
 * @author: zhangbing
 * @create: 2020-07-22 17:31
 **/
public class ZbProducer {

    private TransactionMQProducer transactionMQProducer;

    private DefaultMQProducer mqProducer;


    public ZbProducer(String producerGroup, String nameSvr, boolean isTransaction) {
        if (isTransaction) {
            transactionMQProducer = new TransactionMQProducer(producerGroup);
            mqProducer = transactionMQProducer;
        } else {
            mqProducer = new DefaultMQProducer(producerGroup);
        }
        //配置nameserv的地址
        mqProducer.setNamesrvAddr(nameSvr);
    }


    /**
     * 发送同步单个消息
     *
     * @param msg
     * @return
     */
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

    SendResult send(Message msg, MessageQueueSelector selector, Object args, long timeout) {
        try {
            return mqProducer.send(msg, selector, args, timeout);
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

    /**
     * 发送批量消息
     *
     * @param msgs
     * @return
     */
    SendResult batchSend(List<Message> msgs) {

        try {
            return mqProducer.send(msgs);
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

    /**
     * 发送异步消息
     *
     * @param msgs
     */
    void syncSend(Message msgs) {
        try {
            mqProducer.send(msgs, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println("消息发送成功! result is " + sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    System.out.println("消息发送失败..." + e.getMessage());
                    e.printStackTrace();
                }
            });
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    /**
     * 一次性发送  发送完之后不再管发送结果
     *
     * @param msg
     */
    void sendOneway(Message msg) {
        try {
            mqProducer.sendOneway(msg);
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void shutdown() {
        mqProducer.shutdown();
    }

    void start() {
        try {
            mqProducer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }


    void setTransactionListener(TransactionListener listener) {
        transactionMQProducer.setTransactionListener(listener);
    }

    public void sendMessageInTransaction(Message msg, Object o) {
        try {
            transactionMQProducer.sendMessageInTransaction(msg, o);
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}
