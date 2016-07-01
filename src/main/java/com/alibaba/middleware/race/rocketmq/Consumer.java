package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.RaceUtils;

import java.util.List;


/**
 * Consumer，订阅消息
 */

/**
 * RocketMq消费组信息我们都会再正式提交代码前告知选手
 */
public class Consumer {
	private static int payTimes = 0;
	private static int taobaoTimes = 0;
	private static int tmallTimes = 0;
    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //在本地搭建好broker后,记得指定nameServer的地址
        //consumer.setNamesrvAddr(RaceConfig.NamesrvAddr);

        consumer.subscribe(RaceConfig.MqTaobaoTradeTopic,"*");
        consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
        consumer.subscribe(RaceConfig.MqPayTopic, "*");
       
     consumer.registerMessageListener(new MessageListenerOrderly() {
		
		@Override
		public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
			   for (MessageExt msg : msgs) {
                   byte [] body = msg.getBody();
                   if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                       //Info: 生产者停止生成数据, 并不意味着马上结束
                       System.out.println("-----------------------Got the end signal------------------------------");
                       continue;
                   }
                   if(msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)){
                   	OrderMessage taobaoMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                   //	System.out.println(taobaoTimes ++ +":"+taobaoMessage);
                  	
                   }else if(msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
                      	OrderMessage tmallMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                      //	System.out.println(tmallTimes ++  + ":" + tmallMessage);
                    
                   }else if(msg.getTopic().equals(RaceConfig.MqPayTopic)){
                	   System.err.println(payTimes ++ );
                		PaymentMessage paymeng = RaceUtils.readKryoObject(PaymentMessage.class, body);
                   }
                   
               }
			return ConsumeOrderlyStatus.SUCCESS;
		}
	});
        consumer.start();
        System.out.println("Consumer Started.");
    }
}


//并发

/* 
consumer.registerMessageListener(new MessageListenerConcurrently() {

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                    ConsumeConcurrentlyContext context) {
        for (MessageExt msg : msgs) {

            byte [] body = msg.getBody();
            if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                //Info: 生产者停止生成数据, 并不意味着马上结束
                System.out.println("Got the end signal");
                continue;
            }
            if(msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)){
            	OrderMessage taobaoMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
            	System.out.println(taobaoMessage);
            }else if(msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
               	OrderMessage tmallMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
            	System.out.println(tmallMessage);
            }
            
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
});
*/