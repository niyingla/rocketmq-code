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


	/**
	 * 消费者实际上监听的是 队列Queue 默认均匀分布监听 默认队列四个
	 * 担当携带标签推送的时候 可能因为推送的原因 目标队列可能有消息 没被消费
	 * 例如 消息 10个 分A B tag 连个消费者 1 tag A ,2 tag B 这时候一人监听两个队列
	 * 会因为推送问题 没有消费到对方标签在自己队列的情况 (这种情况广播模式或poll模式可以解决 )
	 *
	 * 最好的就是 topic和tag 绑定不要变动
	 * @param args
	 * @throws MQClientException
	 */
	
	public static void main(String[] args) throws MQClientException {
		//指定消费之组 push常用
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_quick_consumer_name");
		//设置名称服务地址(rocketmq 地址)
		consumer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
		//设置消费点 头部消费 还是尾部消息 等
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
		//订阅topic 订阅表达式(正则tag 过滤) （也可以指定标签 * 表示全部）
		consumer.subscribe("test_quick_topic", "*");
		//消息非配策略 默认平均
		//consumer.setAllocateMessageQueueStrategy();
		//LocalFileOffsetStore 和RemoteBrokerOffsetStore
		//设置标记点
		//consumer.offsetStore()
		//调整线程数
		//consumer.setConsumeThreadMax() consumer.setConsumeThreadMin()
		//单个队列并行消费最大跨度
		//consumer.setConsumeConcurrentlyMaxSpan();
		//一个队列最大消费消息个数
		//consumer.setPullThresholdForQueue();
		//拉群时间间隔
		//consumer.setPullInterval();
		//一次拉取数量
		//consumer.setPullBatchSize();
		//一次消息最多可以拉取多少数据 默认 1
		//consumer.setConsumeMessageBatchMaxSize();
		//最大重复消费次数
		//consumer.setMaxReconsumeTimes();
		//CLUSTERING 集群 BROADCASTING 广播  广播下消息会被每一个consumer消费
		//consumer.setMessageModel();

		//注册消息监听处理 也可以MessageListenerOrderly 顺序消费
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
