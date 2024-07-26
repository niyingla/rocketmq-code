package com.bfxy.rocketmq.consumer.pull;

import com.bfxy.rocketmq.constants.Const;
import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class PullScheduleService {

    public static void main(String[] args) throws MQClientException {

    	String group_name = "test_pull_consumer_name";
    	//定时拉取consumer 可以弥补普通pull存储offset在本地的缺陷
        final MQPullConsumerScheduleService scheduleService = new MQPullConsumerScheduleService(group_name);

        scheduleService.getDefaultMQPullConsumer().setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
        //集群模式（Clustering）和广播模式（Broadcasting）
        scheduleService.setMessageModel(MessageModel.CLUSTERING);
        //注册定时拉取回调函数
        scheduleService.registerPullTaskCallback("test_pull_topic", new PullTaskCallback() {

            @Override
            public void doPullTask(MessageQueue mq, PullTaskContext context) {
                //获取pull（拉） 消费对象
                MQPullConsumer consumer = context.getPullConsumer();
                System.err.println("-------------- queueId: " + mq.getQueueId()  + "-------------");
                try {
                    // 获取从哪里拉取
                    long offset = consumer.fetchConsumeOffset(mq, false);
                    if (offset < 0){
                        offset = 0;
                    }
                    //从offset拉取数据
                    PullResult pullResult = consumer.pull(mq, "*", offset, 32);
                    switch (pullResult.getPullStatus()) {
                    case FOUND:
                        //获取消息列表
                    	List<MessageExt> list = pullResult.getMsgFoundList();
                    	//循环数据
                    	for(MessageExt msg : list){
                    		//消费数据...
                    		System.out.println(new String(msg.getBody()));
                    	}
                        break;
                    case NO_MATCHED_MSG:
                        break;
                    case NO_NEW_MSG:
                    case OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                    }
                    //移动mq队列消费下标 到下一个位置
                    consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
                    // 设置再过3000ms后重新拉取
                    context.setPullNextDelayTimeMillis(3000);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        scheduleService.start();
    }
}
