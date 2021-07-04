package com.bfxy.paya.service.producer;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.bfxy.paya.mapper.CustomerAccountMapper;
@Component
public class TransactionListenerImpl implements TransactionListener {

	@Autowired
	private CustomerAccountMapper customerAccountMapper;

	/**
	 * 提交本地事务
	 * @param msg
	 * @param arg
	 * @return
	 */
	@Override
	public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
		System.err.println("执行本地事务单元------------");
		CountDownLatch currentCountDown = null;
		try {
			Map<String, Object> params = (Map<String, Object>) arg;
			String userId = (String)params.get("userId");
			String accountId = (String)params.get("accountId");
			String orderId = (String)params.get("orderId");
			BigDecimal payMoney = (BigDecimal)params.get("payMoney");	//	当前的支付款
			BigDecimal newBalance = (BigDecimal)params.get("newBalance");	//	前置扣款成功的余额
			int currentVersion = (int)params.get("currentVersion");
			currentCountDown = (CountDownLatch)params.get("currentCountDown");
		
			//updateBalance 传递当前的支付款 数据库操作: 
			Date currentTime = new Date();
			//执行本地数据库操作
			int count = this.customerAccountMapper.updateBalance(accountId, newBalance, currentVersion, currentTime);
			//执行成功 提交
			if(count == 1) {
				currentCountDown.countDown();
				return LocalTransactionState.COMMIT_MESSAGE;
			} else {
				//执行失败回滚
				currentCountDown.countDown();
				return LocalTransactionState.ROLLBACK_MESSAGE;
			}
		} catch (Exception e) {
			e.printStackTrace();
			currentCountDown.countDown();
			return LocalTransactionState.ROLLBACK_MESSAGE;
		}
		
	}

	/**
	 * 检查本地事务
	 * @param msg
	 * @return
	 */
	@Override
	public LocalTransactionState checkLocalTransaction(MessageExt msg) {
		// TODO Auto-generated method stub
		return null;
	}

}
