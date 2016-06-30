package com.alibaba.middleware.race.jstorm.spout;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.StructUtils;
import com.alibaba.middleware.race.SupervisorThread;
import com.alibaba.middleware.race.TopologyUtils;
import com.alibaba.middleware.race.Tair.TairImpl;
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

public class MQSpout implements IRichSpout, IFailValueSpout, MessageListenerOrderly {

	private static final Logger LOG = LoggerFactory.getLogger(MQSpout.class);
	private MessagePushConsumer consumer;
	private SpoutOutputCollector collector;
	private static int emitPayment = 0;
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
		Utils.sleep(1000000);
	}

	public void ack(Object id) {
	}

	public void fail(Object id) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(TopologyUtils.TAOBAO_TOPIC_STREAM, new Fields("msg"));
		declarer.declareStream(TopologyUtils.TMALL_TOPIC_STREAM, new Fields("msg"));
		declarer.declareStream(TopologyUtils.PAY_TOPIC_STREAM, new Fields("msg"));
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
				SupervisorThread.isEnd = true;
				endTime++;
				if (endTime == 3) { // 当pay消费到最后时
					submitFinalMsg();
				}
				continue;
			}
			String msgId = msg.getMsgId();
			if (msg.getTopic().equals(RaceConfig.MqPayTopic)) {
				emitPayment++;
				collector.emit(TopologyUtils.PAY_TOPIC_STREAM, new Values(body));

			} else if (msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)) {
				collector.emit(TopologyUtils.TAOBAO_TOPIC_STREAM, new Values(body));

			} else if (msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)) {
				collector.emit(TopologyUtils.TMALL_TOPIC_STREAM, new Values(body));
			}

		}
		return ConsumeOrderlyStatus.SUCCESS;

	}

	// 提交全部统计结果
	public static void submitFinalMsg() {
		new Thread() {
			public void run() {
				try {
					Thread.sleep(6000);
					LOG.info(
							"Total payment Number is   " + emitPayment + "      Current TaobaoMap、TmallMap size is "
									+ StructUtils.taobaoCacheMap.size() + "  ,  " + StructUtils.tmallCacheMap.size());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				TairImpl.PCSUM = TairImpl.MOBILESUM = 0d;
				for (int i = (int) StructUtils.taobaoDeal[1440]; i < 1440; i++) {
					if (StructUtils.taobaoDeal[i] != 0d) {
						TairImpl.writeAll(i, StructUtils.timeStamp[i]);
					}
				}
				for (int i = 0; i < (int) StructUtils.taobaoDeal[1440]; i++) {
					if (StructUtils.taobaoDeal[i] != 0d) {
						TairImpl.writeAll(i, StructUtils.timeStamp[i]);
					}
				}

			};
		}.start();
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object msgId, List<Object> values) {
		TairImpl.writeFails((String)msgId);
		if (((String) msgId).contains("pay")) {
			collector.emit(TopologyUtils.PAY_TOPIC_STREAM, new Values(values.get(0)), msgId);
		}else if(((String) msgId).contains("tao")){
			collector.emit(TopologyUtils.TAOBAO_TOPIC_STREAM, new Values(values.get(0)), msgId);
		}else if(((String) msgId).contains("tmall")){
			collector.emit(TopologyUtils.TMALL_TOPIC_STREAM, new Values(values.get(0)), msgId);
		}
		values.remove(0);
	}
}
