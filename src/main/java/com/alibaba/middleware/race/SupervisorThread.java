package com.alibaba.middleware.race;

import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.jstorm.spout.MQPayPullSpout;



public class SupervisorThread {
	private static Logger LOG = LoggerFactory.getLogger(SupervisorThread.class);
	public static boolean taobaoSub;
	public static boolean tmallSub;
	public static boolean paySub;
	private long sleepMillions;
	public static boolean isEnd = false;
	public SupervisorThread(){
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
				if((taobaoSize>StructUtils.PAY_OPEN_BOUNDER&&tmallSize>StructUtils.PAY_OPEN_BOUNDER && paySub == false)||(isEnd==true && paySub == false)){
					LOG.info("***************开启pay消费*****************"+ "      Current TaobaoMap、TmallMap size is "
                             + taobaoSize + "  ,  " + tmallSize);
					paySub = true;
				}else if((taobaoSize<StructUtils.PAY_CLOSE_BOUNDER||tmallSize<StructUtils.PAY_CLOSE_BOUNDER)&&paySub ==true&&isEnd==false){
					paySub = false;
				}
				//对Order限流控制
				if(taobaoSize>StructUtils.TAOBAO_UPPER_BOUNDER && taobaoSub==true){
					taobaoSub = false;
					LOG.info("***************开启taobaoOrder限流*****************"+ "      Current TaobaoMap、TmallMap size is "
                            + taobaoSize + "  ,  " + tmallSize);
				}else if(taobaoSize<StructUtils.TAOBAO_LOWER_BOUNDER && taobaoSub == false){
					taobaoSub = true;
					LOG.info("***************开启taobaoOrder开关*****************"+ "      Current TaobaoMap、TmallMap size is "
                            + taobaoSize + "  ,  " + tmallSize);
				}
				
				if(tmallSize>StructUtils.TMALL_UPPER_BOUNDER && tmallSub == true){
					tmallSub = false;
					LOG.info("***************开启tmallOrder限流*****************"+ "      Current TaobaoMap、TmallMap size is "
                            + taobaoSize + "  ,  " + tmallSize);
				}else if(tmallSize<StructUtils.TMALL_LOWER_BOUNDER && tmallSub == false){
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
				MQPayPullSpout.submitFinalMsg();
			}
		}, 1195000l);
	}

	

	

	
	
}
