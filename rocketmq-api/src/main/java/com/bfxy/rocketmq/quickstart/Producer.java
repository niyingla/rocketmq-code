package com.bfxy.rocketmq.quickstart;

import java.util.List;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import com.bfxy.rocketmq.constants.Const;

public class Producer {

	public static void main(String[] args) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		//指定生产组
		DefaultMQProducer producer = new DefaultMQProducer("test_quick_producer_name");
		//设置名称服务地址(rocketmq 地址)
		producer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
		//启动
		producer.start();
		
		for(int i = 0 ; i <5; i ++) {
			//	1.	创建消息 一个topic 对应多个msg queue
			Message message = new Message("test_quick_topic",	//	主题 当前topic 不存在会自动创建
					"TagA", //	标签 标签作用 可用于过滤
					"key" + i,	// 	用户自定义的key ,唯一的标识
					("Hello RocketMQ" + i).getBytes());	//	消息内容实体（byte[]）
			//	2.1	同步发送消息
//			if(i == 1) {
//				message.setDelayTimeLevel(3);
//			}
			//发送消息 发送方法参数很多
			//Message msg 消息, MessageQueueSelector selector, Object arg 参数, SendCallback sendCallback 回调方法, long timeout 发送超时时间
			SendResult sr = producer.send(message, new MessageQueueSelector() {
				@Override
				public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
					Integer queueNumber = (Integer)arg;
					return mqs.get(queueNumber);
				}
			}, 2);

			//输出返回值 里面会有status等
			System.err.println(sr);
			
//			SendResult sr = producer.send(message);
//			SendStatus status = sr.getSendStatus();
//			System.err.println(status);
			
			
			
			//System.err.println("消息发出: " + sr);
			
			//  2.2 异步发送消息
//			producer.send(message, new SendCallback() {
//				//rabbitmq急速入门的实战: 可靠性消息投递
//				@Override
//				public void onSuccess(SendResult sendResult) {
//					System.err.println("msgId: " + sendResult.getMsgId() + ", status: " + sendResult.getSendStatus());
//				}
//				@Override
//				public void onException(Throwable e) {
//					e.printStackTrace();
//					System.err.println("------发送失败");
//				}
//			});
		}

		producer.shutdown();
		
	}
}
