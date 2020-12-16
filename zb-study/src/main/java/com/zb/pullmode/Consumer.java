package com.zb.pullmode;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.zb.constans.Constans.PULL_TOPIC;

/**
 * @description:
 * @author: zhangbing
 * @create: 2020-12-10 15:29
 **/
public class Consumer {

    /**
     * 一个消费者组中的消费者一定要订阅相同的topic下的tags
     * <p>
     * 要是一个消费者组中的 不同消费者 订阅同一个topic下的不同tags 会出现消费不到消息的情况
     *
     * @param args
     * @throws MQClientException
     */
    public static void main(String[] args) throws MQClientException, InterruptedException {


        ThreadPoolExecutor executor = new ThreadPoolExecutor(100, 200, 0, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());


        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("PULL_CONSUMER");//创建消费者
        consumer.setNamesrvAddr("47.100.15.16:9876");//配置namesrv地址
        consumer.subscribe(PULL_TOPIC, "tags-2");//配置订阅的topic和tags
//        consumer.subscribe(SIMPLE_TOPIC, "tags-1||tags-2");//配置多个tags
        consumer.setPullBatchSize(100);
        consumer.setPullThresholdSizeForQueue(200);

        consumer.start();

        while (true) {
            List<MessageExt> poll = consumer.poll();
            System.out.println("处理消息数量:" + poll.size());
            for (MessageExt messageExt : poll) {
                executor.execute(() -> {
                    try {
                        Thread.currentThread().sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("线程:" + Thread.currentThread().getId() + "消费消息：" + new String(messageExt.getBody()));
                });

            }
        }


    }
}
