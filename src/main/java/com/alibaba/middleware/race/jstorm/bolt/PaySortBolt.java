package com.alibaba.middleware.race.jstorm.bolt;

import java.sql.Struct;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.StructUtils;
import com.alibaba.middleware.race.TopologyUtils;
import com.alibaba.middleware.race.Tair.TairImpl;
import com.alibaba.middleware.race.jstorm.RaceTopology;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.esotericsoftware.minlog.Log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * PC/无线、淘宝/天猫的分类、提交通知
 */
public class PaySortBolt implements   IRichBolt{
	private static Logger LOG = LoggerFactory.getLogger(PaySortBolt.class);
	private static OutputCollector collector;
    private static int lastMinFlag = -1;	//上一分钟index
    private static long lastMinStamp = -1;	//上一分钟TimeStamp
    private static int reciveTimes = 3;		//方法一提交时间阈值
    private static final long TIME_FLAG = 30000;	//方法二提交时间阈值
    private static int times=0;
    
	@Override
	public void execute(Tuple input) {
	    if (input.getSourceStreamId().equals(TopologyUtils.PAY_TOPIC_STREAM)) {
	    	PaymentMessage payment =RaceUtils.readKryoObject(PaymentMessage.class, (byte[])input.getValue(0)); 
	    	/**********************判断是否可以提交*************************/
	    	//一天的整分时刻下标
	    	final int index = StructUtils.parseTimeStamp2Int(payment.createTime);
	    	//整分时刻13位时间戳
	    	Long minuteTime = (payment.createTime / 1000 / 60) * 60000;
	    	//变量初始化
	    	if(lastMinFlag==-1){		
	    		StructUtils.taobaoDeal[1440] = index;//第一个pay下标
	    		lastMinFlag = index;
	    		lastMinStamp = minuteTime;
	    	}
	    	StructUtils.timeStamp[index] = minuteTime/1000;//下标-10位时间戳映射
//	    	//方法一:	阈值3分钟
//	    	if(index-StructUtils.taobaoDeal[1440] == reciveTimes){
//	    		reciveTimes++;//相同分钟时间只提交一次
//	    		new Thread(){
//	    			public void run() {
//	    				TairImpl.writeAll(index - 3, StructUtils.timeStamp[index - 3]);
//	    			};
//	    		}.start();
//	    	}
	    	//方法二:	阈值30秒
//	    	if(index!=lastMinFlag && (payment.createTime - minuteTime) > TIME_FLAG&&minuteTime>lastMinStamp){	
//	    		System.err.println("Tair开始结算: Index:"+lastMinFlag+"   	TimeStamp:"+StructUtils.timeStamp[lastMinFlag]);
//	    		TairImpl.writeAll(lastMinFlag, StructUtils.timeStamp[lastMinFlag]);
//	    		lastMinFlag = index;
//	    		lastMinStamp = minuteTime;
//	    	}
	    	/**********************分类*************************/
	    	//0 PC、1 无线
	    	if(payment.payPlatform==0){
	    		collector.emit(TopologyUtils.PC_SORT_STREAM,new Values(payment.payAmount,index));
	    	}else{
	    		collector.emit(TopologyUtils.MOBILE_SORT_STREAM,new Values(payment.payAmount,index));
	    	}
	    	
	    	//根据order map判断taobao 或 tmall 订单。
	    	if(StructUtils.taobaoCacheMap.containsKey(payment.orderId)){
	    		collector.emit(TopologyUtils.TAOBAO_SORT_STREAM,new Values(payment.payAmount,index));
	    		//更新订单总金额,如果全消费完则删除订单
	    		StructUtils.taobaoCacheMap.replace(payment.orderId, (Double)StructUtils.taobaoCacheMap.get(payment.orderId)-payment.payAmount);
	    		if(StructUtils.taobaoCacheMap.get(payment.orderId)<0.01){
	    			StructUtils.taobaoCacheMap.remove(payment.orderId);
	    		}
	    	}else if(StructUtils.tmallCacheMap.containsKey(payment.orderId)){
	    		collector.emit(TopologyUtils.TMALL_SORT_STREAM,new Values(payment.payAmount,index));
	    		//更新订单总金额,如果全消费完则删除订单
	    		StructUtils.tmallCacheMap.replace(payment.orderId, (Double)StructUtils.tmallCacheMap.get(payment.orderId)-payment.payAmount);
	    		if(StructUtils.tmallCacheMap.get(payment.orderId)<0.01){
	    			StructUtils.tmallCacheMap.remove(payment.orderId);
	    		}
	    	}else{
	    		LOG.info("===========pay is faster than order	!!!============");
	    	}
	    }
	    collector.ack(input);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TopologyUtils.PC_SORT_STREAM, new Fields("amount","index"));
        declarer.declareStream(TopologyUtils.MOBILE_SORT_STREAM, new Fields("amount","index"));
        declarer.declareStream(TopologyUtils.TAOBAO_SORT_STREAM, new Fields("amount","index"));
        declarer.declareStream(TopologyUtils.TMALL_SORT_STREAM, new Fields("amount","index"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}


}
