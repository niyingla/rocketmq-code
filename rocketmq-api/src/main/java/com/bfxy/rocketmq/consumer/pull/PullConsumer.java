package com.bfxy.rocketmq.consumer.pull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import com.bfxy.rocketmq.constants.Const;
 

public class PullConsumer {
    /**
     * 一个topic 下默认挂四个队列
     * pull 三件事
     * 1 获取message queue并遍历
     * 2 维护offset
     * 3 根据状态处理数据
     *
     * 写入消息
     * 1 同步刷盘 阻塞写
     * 2 异步刷盘 返回成功 堆积到一定量在写
     *
     * 同步slave 节点
     * 1 同步复制 主节点等待从节点写入
     * 2 异步复制 主节点写入返回成功
     *
     *
     * 推荐同步双写||异步复制
     * nameServer 作用
     * 协调服务，多个节点之间的状态。接受各个broker，上班的状态等。
     * nameServer 之间独立，为了保证热备份，所以部署多个。
     */
	//Map<key, value>  key为指定的队列，value为这个队列拉取数据的最后位置 建议持久化位置信息 以免服务重启
    private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();
 
    public static void main(String[] args) throws MQClientException {
    	
    	String group_name = "test_pull_consumer_name";
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(group_name);
        consumer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
        consumer.start();
        System.err.println("consumer start");
        //	从TopicTest这个主题去获取所有的队列（默认会有4个队列）
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("test_pull_topic");
        //	遍历每一个队列，进行拉取数据
        for (MessageQueue mq : mqs) {
            System.out.println("Consume from the queue: " + mq);
            
            SINGLE_MQ: while (true) {
                try {
                	//	从queue中获取数据，从什么位置开始拉取数据 单次对多拉取32条记录（没数据就阻塞）
                    PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                    System.out.println(pullResult);
                    System.out.println(pullResult.getPullStatus());
                    System.out.println();
                    //修改位置下标
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    //获取当前拉取状态
                    switch (pullResult.getPullStatus()) {
                        //找到了消息
	                    case FOUND:
	                        //获取发现的数组
	                    	List<MessageExt> list = pullResult.getMsgFoundList();
	                    	for(MessageExt msg : list){
	                    	    //打印数据
	                    		System.out.println(new String(msg.getBody()));
	                    	}
	                        break;
                        //没有匹配消息
	                    case NO_MATCHED_MSG:
	                        break;
                        //没有新消息
	                    case NO_NEW_MSG:
	                    	System.out.println("没有新的数据啦...");
	                        break SINGLE_MQ;
                        //下标异常
	                    case OFFSET_ILLEGAL:
	                        break;
	                    default:
	                        break;
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        consumer.shutdown();
    }
 
 
    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        offseTable.put(mq, offset);
    }
 
 
    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = offseTable.get(mq);
        if (offset != null)
            return offset;
        return 0;
    }
 
}