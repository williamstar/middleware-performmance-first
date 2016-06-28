package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;

public class MessageConsumerManager {

    public static MQConsumer getConsumerInstance(MessageListener listener ) throws MQClientException {
	    	
            DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
            //在本地搭建好broker后,记得指定nameServer的地址
          //  pushConsumer.setNamesrvAddr(RaceConfig.NamesrvAddr);
            pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            //顺序消费
            pushConsumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
            pushConsumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
            pushConsumer.registerMessageListener((MessageListenerOrderly) listener);
            return pushConsumer;
    }
}
