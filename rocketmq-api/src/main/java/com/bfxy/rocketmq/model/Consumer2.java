package com.bfxy.rocketmq.model;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import com.bfxy.rocketmq.constants.Const;

public class Consumer2 {

	public Consumer2() {
		try {
			String group_name = "test_model_consumer_name2";
			/**
			 * 长轮训 发请求拉取消息 没消息阻塞 默认15s
			 */
			DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
			consumer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
			//多个tag 消费 tagA||tagB
			consumer.subscribe("test_model_topic2", "TagB");
			//consumer.setMessageModel(MessageModel.CLUSTERING);
			consumer.setMessageModel(MessageModel.BROADCASTING);
			consumer.registerMessageListener(new Listener());
			consumer.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	class Listener implements MessageListenerConcurrently {

		@Override
		public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
			try {
				for(MessageExt msg : msgs){
					String topic = msg.getTopic();
					String msgBody = new String(msg.getBody(),"utf-8");
					String tags = msg.getTags();
					//if(tags.equals("TagB")) {
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
		Consumer2 c2 = new Consumer2();
		System.out.println("c2 start..");
		
	}
}
