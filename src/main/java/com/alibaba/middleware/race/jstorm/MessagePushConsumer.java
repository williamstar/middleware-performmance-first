package com.alibaba.middleware.race.jstorm;

import java.io.Serializable;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;


public class MessagePushConsumer implements Serializable {

    private transient DefaultMQPushConsumer consumer;

    public void start(MessageListener listener) throws Exception {
        consumer = (DefaultMQPushConsumer) MessageConsumerManager.getConsumerInstance(listener);
        this.consumer.start();
    }


    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }
}
