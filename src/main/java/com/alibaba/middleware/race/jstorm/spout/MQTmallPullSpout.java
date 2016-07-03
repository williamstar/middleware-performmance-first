package com.alibaba.middleware.race.jstorm.spout;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.middleware.race.QueueOffsetCache;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.SupervisorThread;
import com.alibaba.middleware.race.Tair.TairImpl;
import com.alibaba.middleware.race.jstorm.PullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MQTmallPullSpout implements IRichSpout, IFailValueSpout {
	private static final Logger LOG = LoggerFactory.getLogger(MQTmallPullSpout.class);
	private DefaultMQPullConsumer consumer;
	private SpoutOutputCollector collector;
	private Set<MessageQueue> mqs;// 根据topic获取对应的MessageQueue
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		if (consumer == null) {
			consumer = PullConsumer.getInstance().getConsumer();
			try {
				mqs = consumer.fetchSubscribeMessageQueues(RaceConfig.MqTmallTradeTopic);
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
		// 如果tmall开关为打开状态
		if (SupervisorThread.tmallSub) {
			handleAndEmit();
		}
	}
	
	private void handleAndEmit() {
        for (MessageQueue mq : mqs) {
            try {
                PullResult pullResult = consumer.pull(mq,
                        null, QueueOffsetCache.getMessageQueueOffset(mq),
                        RaceConfig.PULLBATCHSIZE);
                QueueOffsetCache.putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        sendMsg(pullResult.getMsgFoundList());
                        break;
                    case OFFSET_ILLEGAL:
                        throw new IllegalStateException("Illegal offset " + pullResult);
                    default:
//                        LOG.warn("Unconcerned status {} for result {} !",
//                                pullResult.getPullStatus(), pullResult);
                        break;
                }
            } catch (Exception e) {
                LOG.error("Error occurred in queue {} !", new Object[]{mq}, e);
            }
        }
    }
	
	  /**
     * 发送TmallOrder消息
     * @param msgs
     */
    private void sendMsg(List<MessageExt> msgs){
    	for (MessageExt msg : msgs) {
			// 发送fail时重发
			byte[] body = msg.getBody();
			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				// 当order消费到最后时,之后可能还有数据
				SupervisorThread.isEnd = true;
				continue;
			}
			collector.emit(new Values(body));
		}
    }
	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub

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
		TairImpl.writeFails("tmall_"+(String)msgId);
		collector.emit(new Values(values.get(0)), msgId);
		values.remove(0);
	}

}
