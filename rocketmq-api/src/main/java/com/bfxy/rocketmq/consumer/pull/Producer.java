package com.bfxy.rocketmq.consumer.pull;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import com.bfxy.rocketmq.constants.Const;
 
public class Producer {

    /**
     * 普通发送消息
     * 设置 topic 默认队列数
     * send  还可以设置消息发送超时时间（ms）
     * @param args
     * @throws MQClientException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws MQClientException, InterruptedException {
        //当前应用必须唯一
    	String group_name = "test_pull_producer _name";
        DefaultMQProducer producer = new DefaultMQProducer(group_name);
        producer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
        //创建topic
//        producer.createTopic();
        //设置默认topic关联队列个数
//        producer.setDefaultTopicQueueNums();
        //key topic queueNum 堆积队列数
//        producer.createTopic();
        //心跳频率
//        producer.setHeartbeatBrokerInterval();
        //
//        producer.setInstanceName();
        //发送超时失败 可重试
//        producer.setRetryTimesWhenSendAsyncFailed();
        //没存储成功是否可以对其他broke进行存储 默认false
//        producer.isRetryAnotherBrokerWhenNotStoreOK()
        //默认压缩字节起点
//        producer.setDefaultTopicQueueNums();
        //发送超时时间
//        producer.setSendMsgTimeout();
        //最大消息限制 默认128k
//        producer.setMaxMessageSize();
        producer.start();
 
        for (int i = 0; i < 10; i++) {
            try {
                Message msg = new Message("test_pull_topic",// topic
                		"TagA",// tag
                		("Hello RocketMQ " + i).getBytes()// body
                );

                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
                Thread.sleep(1000);
            }
            catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(3000);
            }
        }
 
        producer.shutdown();
    }
}
