package com.alibaba.middleware.race.jstorm.bolt;

import java.io.Serializable;
import java.util.Map;


import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SerializableBolt implements IBasicBolt,Serializable {
	
	private static final long serialVersionUID = 5254328388455387468L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("id","amount","flag"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		int flag = (int)input.getValue(1);
		if(flag==-1){
			collector.emit(new Values(-1,-1,-1));//pay結束
			return;
		}
		if (flag == 0) {
			OrderMessage taobaoOrder = RaceUtils.readKryoObject(OrderMessage.class, (byte[]) input.getValue(0));
			collector.emit(new Values(taobaoOrder.orderId,taobaoOrder.totalPrice,0));
		} else if (flag == 1) {
			OrderMessage tmallOrder = RaceUtils.readKryoObject(OrderMessage.class, (byte[]) input.getValue(0));
			collector.emit(new Values(tmallOrder.orderId,tmallOrder.totalPrice,1));
		} else if(flag == 2){
			PaymentMessage payment = RaceUtils.readKryoObject(PaymentMessage.class, (byte[]) input.getValue(0));
			int index = RaceUtils.parseTimeStamp2Int(payment.createTime);
			collector.emit(new Values(payment,index,2));
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1) {

	}

}
