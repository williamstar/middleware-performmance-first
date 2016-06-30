package com.alibaba.middleware.race.jstorm;

import java.io.Serializable;


import com.alibaba.middleware.race.SupervisorThread;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;


public class MessagePushConsumer implements Serializable {

    private transient DefaultMQPushConsumer consumer;

    public void start(MessageListener listener) throws Exception {
        consumer = (DefaultMQPushConsumer) MessageConsumerManager.getConsumerInstance(listener);
//        consumer.setConsumeMessageBatchMaxSize(64);
//        consumer.setConsumeThreadMax(4);
        new SupervisorThread(consumer).TimeWork();	//	监视线程
        SupervisorThread.timeEmit();
        this.consumer.start();
    }

    public void shutdown() {
        consumer.shutdown();

    }

    public void suspend() {
        consumer.suspend();

    }

    public void resume() {
        consumer.resume();

    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }
}
