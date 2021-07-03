package com.bfxy.rocketmq.constants;

public class Const {

	/**
	 * 主服务服务关闭后 从服务被消息
	 * 再次启动主服务 会去同步已经消费消息节点 防止重复消息
	 */

	public static final String NAMESRV_ADDR_SINGLE = "192.168.11.81:9876";
	
	public static final String NAMESRV_ADDR_MASTER_SLAVE = "192.168.11.81:9876;192.168.11.82:9876";
}
