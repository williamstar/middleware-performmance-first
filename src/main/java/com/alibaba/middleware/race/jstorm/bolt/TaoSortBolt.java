package com.alibaba.middleware.race.jstorm.bolt;

import java.util.Map;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.StructUtils;
import com.alibaba.middleware.race.TopologyUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TaoSortBolt implements IRichBolt {
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		if (input.getSourceStreamId().equals(TopologyUtils.TAOBAO_TOPIC_STREAM)) {
			OrderMessage taobaoOrder = RaceUtils.readKryoObject(OrderMessage.class, (byte[]) input.getValue(0));
			StructUtils.taobaoCacheMap.put(taobaoOrder.orderId, taobaoOrder.totalPrice);
			
		}
//	    collector.ack(input);
	}

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

}
