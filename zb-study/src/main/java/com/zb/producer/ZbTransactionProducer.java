package com.zb.producer;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @description: 事务消息
 * @author: zhangbing
 * @create: 2020-07-24 15:09
 **/
public class ZbTransactionProducer {

    public static void main(String[] args) {
        ZbProducer producer = new ZbProducer("my-txproducerGroup", "127.0.0.1:9876", true);

        producer.setTransactionListener(new TransactionListener() {

            //事务消息回调
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                LocalTransactionState state = null;
                //msg-4返回COMMIT_MESSAGE
                if(msg.getKeys().equals("msg-1")){
                    state = LocalTransactionState.COMMIT_MESSAGE;//msg-1 消息可以提交
                }
                //msg-5返回ROLLBACK_MESSAGE
                else if(msg.getKeys().equals("msg-2")){
                    state = LocalTransactionState.ROLLBACK_MESSAGE;//msg-2 消息回滚
                }else{
                    //这里返回unknown的目的是模拟执行本地事务突然宕机的情况（或者本地执行成功发送确认消息失败的场景）
                    state = LocalTransactionState.UNKNOW; // 其他消息需要回查事务
                }
                System.out.println(msg.getKeys() + ",state:" + state);
                return state;
            }

            //事务消息回查
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                if (null != msg.getKeys()) {
                    switch (msg.getKeys()) {
                        case "msg-3":
                            System.out.println("msg-3 COMMIT_MESSAGE");
                            return LocalTransactionState.COMMIT_MESSAGE;//msg-3 事务结果未知 继续回查消息
                        case "msg-4":
                            System.out.println("msg-4 COMMIT_MESSAGE");
                            return LocalTransactionState.COMMIT_MESSAGE;// msg-4 事务结果可以commit
                        case "msg-5":
                            //查询到本地事务执行失败，需要回滚消息。
                            System.out.println("msg-5 ROLLBACK_MESSAGE");
                            return LocalTransactionState.ROLLBACK_MESSAGE;//msg-5 事务结果需要roll back
                    }
                }
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        //启动事务
        producer.start();

        //发送事务消息

//        for (int i=1;i<=5;i++){
//            Message msg = new Message("transactionTopic", null, "msg-" + i, ("测试，这是事务消息！ " + i).getBytes());
//            producer.sendMessageInTransaction(msg, null);
//        }


    }
}
