package com.zb.sendoneway;

import com.zb.constans.Constans;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @description:
 * @author: zhangbing
 * @create: 2020-12-10 18:14
 **/
public class OneWayProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        // 指定生产组名为my-producer
        DefaultMQProducer producer = new DefaultMQProducer("oneway_group");
        // 配置namesrv地址
        producer.setNamesrvAddr("47.100.15.16:9876");
        // 启动Producer
        producer.start();

        // 创建消息对象，topic为：myTopic001，消息内容为：hello world oneway
        Message msg = new Message(Constans.ONEWAY_TOPIC, "hello world oneway".getBytes());
        // 效率最高，因为oneway不关心是否发送成功，我就投递一下我就不管了。所以返回是void
        producer.sendOneway(msg);
        System.out.println("投递消息成功！，注意这里是投递成功，而不是发送消息成功哦！因为我sendOneway也不知道到底成没成功，我没返回值的。");
        producer.shutdown();
    }
}
