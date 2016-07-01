package com.alibaba.middleware.race.jstorm.spout;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.middleware.race.QueueOffsetCache;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.StructUtils;
import com.alibaba.middleware.race.SupervisorThread;
import com.alibaba.middleware.race.Tair.TairImpl;
import com.alibaba.middleware.race.jstorm.PullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

import backtype.storm.command.list;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class MQPayPullSpout implements IRichSpout, IFailValueSpout {
	private static final Logger LOG = LoggerFactory.getLogger(MQPayPullSpout.class);
	private DefaultMQPullConsumer consumer;
	private SpoutOutputCollector collector;
	private Set<MessageQueue> mqs;// 根据topic获取对应的MessageQueue
	private static int payNum = 0;
	private final static int SLEEPTIME = 10000;// 10s

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		if (consumer == null) {
			consumer = PullConsumer.getInstance().getConsumer();
			try {
				mqs = consumer.fetchSubscribeMessageQueues(RaceConfig.MqPayTopic);
			} catch (MQClientException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void close() {
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public void nextTuple() {
		// 如果pay开关为打开状态
		if (SupervisorThread.paySub) {
			handleAndEmit();
		}
	}

	private void handleAndEmit() {
		for (MessageQueue mq : mqs) {
			try {
				PullResult pullResult = consumer.pull(mq, null,
						QueueOffsetCache.getMessageQueueOffset(mq), RaceConfig.PULLBATCHSIZE);
				QueueOffsetCache.putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
				switch (pullResult.getPullStatus()) {
				case FOUND:
					sendMsg(pullResult.getMsgFoundList());
					break;
				case OFFSET_ILLEGAL:
					throw new IllegalStateException("Illegal offset " + pullResult);
				default:
					//LOG.warn("Unconcerned status {} for result {} !", pullResult.getPullStatus(), pullResult);
					break;
				}
			} catch (Exception e) {
				LOG.error("Error occurred in queue {} !", new Object[] { mq }, e);
			}
		}
	}

	/**
	 * 发送pay消息
	 * 
	 * @param msgs
	 */
	private void sendMsg(List<MessageExt> msgs) {
		for (MessageExt msg : msgs) {
			// 发送fail时重发
			byte[] body = msg.getBody();
			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				// 当pay消费到最后时,之后可能还有数据
				submitFinalMsg();
				continue;
			}
			payNum++;
			collector.emit(new Values(body));
		}
	}

	/**
	 * 当订阅到最后时,提交结果
	 */
	public static void submitFinalMsg() {
		new Thread() {
			public void run() {
				try {
					Thread.sleep(SLEEPTIME);
					LOG.info("Total payment Number is   " + payNum + "。      Current TaobaoMap、TmallMap size is "
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
	public void ack(Object msgId) {
	}

	@Override
	public void fail(Object msgId) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("msg"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void fail(Object msgId, List<Object> values) {
		TairImpl.writeFails("pay_" + (String) msgId);
		collector.emit(new Values(values.get(0)), msgId);
		values.remove(0);
	}

}
