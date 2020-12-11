package com.zb.transaction;

import com.zb.constans.Constans;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.CountDownLatch;

/**
 * @description: 事务消息发送
 * @author: zhangbing
 * @create: 2020-12-11 13:58
 **/
public class TransactionProducer {


    public static void main(String[] args) throws MQClientException, InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        TransactionMQProducer producer = new TransactionMQProducer("trancation_group");
        producer.setNamesrvAddr("47.100.15.16:9876");

        //配置事务监听器
        producer.setTransactionListener(new TransactionListener() {

            //half message投递成功 执行本地事务
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                System.out.println("执行本地事务");
                //执行本地事务
                System.out.println(arg);
                switch (msg.getTags()) {

                    case "tags-1":
                        System.out.println(msg.getTags() + ":本地事务执行成功,提交消息");
                        return LocalTransactionState.COMMIT_MESSAGE;//事务执行成功 提交消息
                    case "tags-2":
                        System.out.println(msg.getTags() + ":本地事务执行失败,回滚消息");
                        return LocalTransactionState.ROLLBACK_MESSAGE;//本地事务执行失败 回滚消息
                    default:
                        System.out.println(msg.getTags() + ":无法找到的tags来源，无法处理本地事务");
                        return LocalTransactionState.UNKNOW;
                }
            }

            //rocketmq 未收到消息的commit/rollback请求，回调此方法查询本地事务
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                System.out.println("事务回查");
                countDownLatch.countDown();
                if (msg.getTags().equals("tags-3")) {
                    System.out.println(msg.getTags() + ":本地事务已被执行，可以提交消息");
                    return LocalTransactionState.COMMIT_MESSAGE;
                }
                return LocalTransactionState.UNKNOW;
            }
        });

        producer.start();

        for (int i = 1; i < 4; i++) {
            Message message = new Message(Constans.TRANSACTION_TOPIC, "tags-" + i, ("事务消息：" + i).getBytes());
            producer.sendMessageInTransaction(message, "我是额外的参数");
        }

        countDownLatch.await();
        producer.shutdown();

    }

}
