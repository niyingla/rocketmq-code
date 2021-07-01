package com.bfxy.rocketmq.quickstart;

import java.util.List;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import com.bfxy.rocketmq.constants.Const;

public class Consumer {

	
	public static void main(String[] args) throws MQClientException {
		//指定消费之组 push常用
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_quick_consumer_name");
		//设置名称服务地址(rocketmq 地址)
		consumer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
		//设置消费点 头部消费 还是尾部消息 等
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
		//订阅topic 订阅表达式 （也可以指定标签 * 表示全部）
		consumer.subscribe("test_quick_topic", "*");
		//注册消息监听处理
		consumer.registerMessageListener(new MessageListenerConcurrently() {

			//msgs中只收集同一个topic，同一个tag，并且key相同的message
			//会把不同的消息分别放置到不同的队列中
			//context 全局上下文对象
			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				//
				MessageExt me = msgs.get(0);
				try {
					//对应topic
					String topic = me.getTopic();
					//消息标签
					String tags = me.getTags();
					//消息key
					String keys = me.getKeys();
//					if(keys.equals("key1")) {
//						System.err.println("消息消费失败..");
//						int a = 1/0;
//					}

					//字节码 转string
					String msgBody = new String(me.getBody(), RemotingHelper.DEFAULT_CHARSET);
					System.err.println("topic: " + topic + ",tags: " + tags + ", keys: " + keys + ",body: " + msgBody);
				} catch (Exception e) {
					e.printStackTrace();
					//重试次数
//					int recousumeTimes = me.getReconsumeTimes();
//					System.err.println("recousumeTimes: " + recousumeTimes);
//					if(recousumeTimes == 3) {
//						//		记录日志....
//						//  	做补偿处理
//						return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//					}
					//需要重新消费
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
				//返回状态
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});

		//开始
		consumer.start();
		System.err.println("consumer start...");
		
	}
}
