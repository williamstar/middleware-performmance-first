package com.alibaba.middleware.race.jstorm.bolt;


import java.util.Map;

import com.alibaba.middleware.race.StructUtils;
import com.alibaba.middleware.race.TopologyUtils;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class PCCountBolt implements   IBasicBolt{

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(input.getSourceStreamId().equals(TopologyUtils.PC_SORT_STREAM)){
				double pay = input.getDouble(0);
				int index = input.getInteger(1);
				StructUtils.PCDeal[index] += pay;
			}
	}

}
