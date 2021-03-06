package com.bfxy.paya.service.producer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.tomcat.util.threads.ThreadPoolExecutor;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TransactionProducer implements InitializingBean {

	//事务消息生产对象
	private TransactionMQProducer producer;
	
	private ExecutorService executorService;
	
	@Autowired
	private TransactionListenerImpl transactionListenerImpl;
	
	private static final String NAMESERVER = "192.168.11.121:9876;192.168.11.122:9876;192.168.11.123:9876;192.168.11.124:9876";
	
	private static final String PRODUCER_GROUP_NAME = "tx_pay_producer_group_name";
	
	private TransactionProducer() {
		//事务消息生产者
		this.producer = new TransactionMQProducer(PRODUCER_GROUP_NAME);
		//手动创建线程池
		this.executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
				new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread thread = new Thread(r);
						thread.setName(PRODUCER_GROUP_NAME + "-check-thread");
						return thread;
					}
				});
		this.producer.setExecutorService(executorService);
		this.producer.setNamesrvAddr(NAMESERVER);
	}

	/**
	 * 初始化后执行
	 * @throws Exception
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		//初始化 设置事务消息提交监听者
		this.producer.setTransactionListener(transactionListenerImpl);
		start();
	}

	/**
	 * 启动事务消息生产客户端
	 */
	private void start() {
		try {
			this.producer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
		}
	}
	
	public void shutdown() {
		this.producer.shutdown();
	}

	/**
	 * 发送事务消息方法
	 * @param message
	 * @param argument
	 * @return
	 */
	public TransactionSendResult sendMessage(Message message, Object argument) {
		TransactionSendResult sendResult = null;
		try {
			//调用之前设置好的事务消息生产者发送事务消息
			sendResult = this.producer.sendMessageInTransaction(message, argument);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sendResult;
	}
	
	
	
	
	
	
	
	
	
	
	
}
