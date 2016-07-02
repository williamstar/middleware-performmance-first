package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;

public final class PullConsumer {
	private static PullConsumer consumer = null;
	private DefaultMQPullConsumer pullConsumer;
	public DefaultMQPullConsumer getConsumer(){
		return pullConsumer;
	}
	private PullConsumer(){
		pullConsumer = new DefaultMQPullConsumer(RaceConfig.MetaConsumerGroup);
//		 pullConsumer.setNamesrvAddr(RaceConfig.NamesrvAddr);
		try {
			pullConsumer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
		}
	}
	public static PullConsumer getInstance (){
	     if(null == consumer) { 
             synchronized(PullConsumer.class){ 
                    if(null == consumer)
                    	consumer = new PullConsumer(); 
             } 
        } 
		return consumer;
	}
}
