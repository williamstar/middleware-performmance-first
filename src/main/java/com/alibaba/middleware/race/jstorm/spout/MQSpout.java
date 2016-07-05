package com.alibaba.middleware.race.jstorm.spout;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.MessagePushConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class MQSpout implements IRichSpout, IFailValueSpout,MessageListenerOrderly,Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(MQSpout.class);
	private MessagePushConsumer consumer;
	private SpoutOutputCollector collector;
	private static int endTime = 0;
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		if (consumer == null) {
			try {
				consumer = new MessagePushConsumer();
				consumer.start(this);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	public void close() {
	}

	public void nextTuple() {
	}

	public void ack(Object id) {
	}

	public void fail(Object id) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("msg","flag"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public DefaultMQPushConsumer getConsumer() {
		return consumer.getConsumer();
	}

	/**
	 * 分成taobaoTopic、tmallTopic、payTopic
	 */
	@Override
	public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
		for (MessageExt msg : msgs) {
			// 发送fail时重发
			byte[] body = msg.getBody();
			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				// 生产者停止生成数据, 并不意味着马上结束
				endTime++;
				if (endTime == 3) { // 当pay消费到最后时
					Utils.sleep(5000);
					collector.emit(new Values(null,-1));
				}
				continue;
			}
			if (msg.getTopic().equals(RaceConfig.MqPayTopic)) {
				collector.emit(new Values(body,2));

			} else if (msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)) {
				collector.emit(new Values(body,0));

			} else if (msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)) {
				collector.emit(new Values(body,1));
			}

		}
		return ConsumeOrderlyStatus.SUCCESS;

	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public void fail(Object msgId, List<Object> values) {
		collector.emit(new Values(values.get(0)));
		values.remove(0);
	}
}
