package com.bfxy.rocketmq.model;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import com.bfxy.rocketmq.constants.Const;

public class Consumer1 {

	public Consumer1() {
		try {
			//消费者组名称相同 能组成一个集群
			String group_name = "test_model_consumer_name1";
			DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
			consumer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
			consumer.subscribe("test_model_topic2", "TagA");
			//consumer.setMessageModel(MessageModel.CLUSTERING);
			consumer.setMessageModel(MessageModel.BROADCASTING);
			consumer.registerMessageListener(new Listener());
			consumer.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	class Listener implements MessageListenerConcurrently {
		public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
			try {
				for(MessageExt msg : msgs){
					String topic = msg.getTopic();
					String msgBody = new String(msg.getBody(),"utf-8");
					String tags = msg.getTags();
					//if(tags.equals("TagA")) {
						System.out.println("收到消息：" + "  topic :" + topic + "  ,tags : " + tags + " ,msg : " + msgBody);
					//}				
				}
			} catch (Exception e) {	
				e.printStackTrace();
				return ConsumeConcurrentlyStatus.RECONSUME_LATER;
			}			
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		}
		
	}
	
	public static void main(String[] args) {
		Consumer1 c1 = new Consumer1();
		System.out.println("c1 start..");
		
	}
}
