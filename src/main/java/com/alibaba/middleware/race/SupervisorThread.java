package com.alibaba.middleware.race;

import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.jstorm.spout.MQSpout;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;


public class SupervisorThread {
	private static Logger LOG = LoggerFactory.getLogger(SupervisorThread.class);
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
	
	public void TimeWork(){
		new Timer().schedule(new TimerTask() {
			@Override
			public void run() {
				int taobaoSize = StructUtils.taobaoCacheMap.size();
				int tmallSize = StructUtils.tmallCacheMap.size();
				//超过某值开启pay消费
				if(taobaoSize+tmallSize >StructUtils.LOWER_BOUNDER && paySub == false){
					LOG.info("***************开启pay消费*****************"+ "      Current TaobaoMap、TmallMap size is "
                             + taobaoSize + "  ,  " + tmallSize);
					try {
						consumer.subscribe(RaceConfig.MqPayTopic, "*");
					} catch (MQClientException e) {
						e.printStackTrace();
					}
					paySub = true;
				}
				//对Order限流控制
				if(taobaoSize>StructUtils.TAOBAO_UPPER_BOUNDER && taobaoSub==true){
					consumer.unsubscribe(RaceConfig.MqTaobaoTradeTopic);
					taobaoSub = false;
					LOG.info("***************开启taobaoOrder限流*****************"+ "      Current TaobaoMap、TmallMap size is "
                            + taobaoSize + "  ,  " + tmallSize);
				}else if(taobaoSize<StructUtils.TAOBAO_LOWER_BOUNDER && taobaoSub == false){
					try {
						consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
					} catch (MQClientException e) {
						e.printStackTrace();
					}
					taobaoSub = true;
					LOG.info("***************开启taobaoOrder开关*****************"+ "      Current TaobaoMap、TmallMap size is "
                            + taobaoSize + "  ,  " + tmallSize);
				}
				
				if(tmallSize>StructUtils.TMALL_UPPER_BOUNDER && tmallSub == true){
					consumer.unsubscribe(RaceConfig.MqTmallTradeTopic);
					tmallSub = false;
					LOG.info("***************开启tmallOrder限流*****************"+ "      Current TaobaoMap、TmallMap size is "
                            + taobaoSize + "  ,  " + tmallSize);
				}else if(tmallSize<StructUtils.TMALL_LOWER_BOUNDER && tmallSub == false){
					try {
						consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
					} catch (MQClientException e) {
						e.printStackTrace();
					}
					tmallSub = true;
					LOG.info("***************开启tmallOrder开关*****************"+ "      Current TaobaoMap、TmallMap size is "
                            + taobaoSize + "  ,  " + tmallSize);
				}
						
			}
		}, 0, sleepMillions);
	}
	

	//19:55 emit
	public static void timeEmit(){
		new Timer().schedule(new TimerTask() {
			@Override
			public void run() {
				MQSpout.submitFinalMsg();
			}
		}, 1195000l);
	}

	

	

	
	
}
