package com.alibaba.middleware.race;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.jstorm.RaceTopology;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import backtype.storm.utils.Utils;

public class SupervisorThread implements Runnable {
	
	private DefaultMQPushConsumer consumer;
	private boolean taobaoSub;
	private boolean tmallSub;
	private boolean paySub;
	private long sleepMillions;
	
	public SupervisorThread(DefaultMQPushConsumer consumer){
		this.consumer = consumer;
		this.taobaoSub = false;
		this.tmallSub = false;
		this.paySub = false;
		this.sleepMillions = 50;
	}
	@Override
	public void run() {
		while(true){
			if( getOrderSum() >= StructUtils.UPPER_BOUNDER){
				if(!paySub){
					try {
						this.consumer.subscribe(RaceConfig.MqPayTopic, "*");
						this.paySub = true;
					} catch (MQClientException e) {
						e.printStackTrace();
					}
					if(this.taobaoSub)
						unsubOrder();
				}
			}else if( getOrderSum() < StructUtils.LOWER_BOUNDER&&this.taobaoSub==false){
					subOrder();
			}
			Utils.sleep(sleepMillions);
		}
	}
	
	public int getOrderSum(){
		return StructUtils.taobaoCacheMap.size() + StructUtils.tmallCacheMap.size();
	}
	
	public void subOrder(){
		try {
			this.consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
			this.consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
			this.tmallSub = true;
			this.taobaoSub = true;
		} catch (MQClientException e) {
			e.printStackTrace();
		}
	}
	
	public void unsubOrder(){
		this.consumer.unsubscribe(RaceConfig.MqTaobaoTradeTopic);
		this.consumer.unsubscribe(RaceConfig.MqTmallTradeTopic);
		this.tmallSub = false;
		this.taobaoSub = false;
	}
	
	
}
