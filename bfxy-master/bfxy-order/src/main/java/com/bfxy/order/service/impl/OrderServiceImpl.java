package com.bfxy.order.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.dubbo.config.annotation.Reference;
import com.bfxy.order.constants.OrderStatus;
import com.bfxy.order.entity.Order;
import com.bfxy.order.mapper.OrderMapper;
import com.bfxy.order.service.OrderService;
import com.bfxy.order.service.producer.OrderlyProducer;
import com.bfxy.order.utils.FastJsonConvertUtil;
import com.bfxy.store.service.StoreServiceApi;

@Service
public class OrderServiceImpl implements OrderService {

	@Autowired
	private OrderMapper orderMapper;
	
    @Reference(version = "1.0.0",
            application = "${dubbo.application.id}",
            interfaceName = "com.bfxy.store.service.StoreServiceApi",
            check = false,
            timeout = 3000,
            retries = 0
    )
	private StoreServiceApi storeServiceApi;
    
    @Autowired
    private OrderlyProducer orderlyProducer;
	
	@Override
	public boolean createOrder(String cityId, String platformId, String userId, String supplierId, String goodsId) {
		boolean flag = true;
		try {
			Order order = new Order();
			order.setOrderId(UUID.randomUUID().toString().substring(0, 32));
			order.setOrderType("1");
			order.setCityId(cityId);
			order.setPlatformId(platformId);
			order.setUserId(userId);
			order.setSupplierId(supplierId);
			order.setGoodsId(goodsId);
			order.setOrderStatus(OrderStatus.ORDER_CREATED.getValue());
			order.setRemark("");
			
			Date currentTime = new Date();
			order.setCreateBy("admin");
			order.setCreateTime(currentTime);
			order.setUpdateBy("admin");
			order.setUpdateTime(currentTime);
			
			int currentVersion = storeServiceApi.selectVersion(supplierId, goodsId);
			int updateRetCount = storeServiceApi.updateStoreCountByVersion(currentVersion, supplierId, goodsId, "admin", currentTime);
			
			if(updateRetCount == 1) {
				// DOTO:	????????????SQL?????? ????????????, ???????????? ??????????????? ??????????????????????????????
				orderMapper.insertSelective(order);
			} 
			//	?????????????????? 1 ???????????????????????????   2 ????????????
			else if (updateRetCount == 0){
				flag = false;	//	??????????????????
				int currentStoreCount = storeServiceApi.selectStoreCount(supplierId, goodsId);
				if(currentStoreCount == 0) {
					//{flag:false , messageCode: 003 , message: ??????????????????}
					System.err.println("-----??????????????????...");
				} else {
					//{flag:false , messageCode: 004 , message: ???????????????}
					System.err.println("-----???????????????...");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			// 	????????????????????????????????????
			flag = false;
		}
		
		return flag;
	}

	public static final String PKG_TOPIC = "pkg_topic";
	
	public static final String PKG_TAGS = "pkg";
	@Override
	public void sendOrderlyMessage4Pkg(String userId, String orderId) {
		List<Message> messageList = new ArrayList<>();
		
		Map<String, Object> param1 = new HashMap<String, Object>();
		param1.put("userId", userId);
		param1.put("orderId", orderId);
		param1.put("text", "??????????????????---1");
		
		String key1 = UUID.randomUUID().toString() + "$" +System.currentTimeMillis();
		Message message1 = new Message(PKG_TOPIC, PKG_TAGS, key1, FastJsonConvertUtil.convertObjectToJSON(param1).getBytes());
		
		messageList.add(message1);
		
		
		Map<String, Object> param2 = new HashMap<String, Object>();
		param2.put("userId", userId);
		param2.put("orderId", orderId);
		param2.put("text", "????????????????????????---2");
		
		String key2 = UUID.randomUUID().toString() + "$" +System.currentTimeMillis();
		Message message2 = new Message(PKG_TOPIC, PKG_TAGS, key2, FastJsonConvertUtil.convertObjectToJSON(param2).getBytes());
		
		messageList.add(message2);
		
		//	?????????????????? ??????????????? ?????????ID ???topic ??? messagequeueId ?????????????????????
		//  supplier_id
		
		Order order = orderMapper.selectByPrimaryKey(orderId);
		int messageQueueNumber = Integer.parseInt(order.getSupplierId());
		
		//????????????????????????????????? ???messageList ?????????
		orderlyProducer.sendOrderlyMessages(messageList, messageQueueNumber);
	}

	
}
