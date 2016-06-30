package com.alibaba.middleware.race.rocketmq;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

public class PullConsumer {

	private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();

	public static void main(String[] args) throws MQClientException {

		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(RaceConfig.MetaConsumerGroup);

		consumer.setNamesrvAddr("127.0.0.1:9876");

		consumer.start();

		Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(RaceConfig.MqTaobaoTradeTopic);
		

		for (MessageQueue mq : mqs) {

			System.out.println("Consume from the queue: " + mq);

			SINGLE_MQ: while (true) {

				try {

					PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);

					System.out.println(pullResult);

					putMessageQueueOffset(mq, pullResult.getNextBeginOffset());

					switch (pullResult.getPullStatus()) {

					case FOUND:

						// TODO
						List<MessageExt> list = pullResult.getMsgFoundList();
						for(MessageExt message : list){
							System.out.println("主题是"+message.getTopic());
							OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class, message.getBody());
							System.out.println(order);
						}

						break;

					case NO_MATCHED_MSG:

						break;

					case NO_NEW_MSG:

						break SINGLE_MQ;

					case OFFSET_ILLEGAL:

						break;

					default:

						break;

					}

				}

				catch (Exception e) {

					e.printStackTrace();

				}

			}

		}

		consumer.shutdown();

	}

	private static void putMessageQueueOffset(MessageQueue mq, long offset) {

		offseTable.put(mq, offset);

	}

	private static long getMessageQueueOffset(MessageQueue mq) {
		Long offset = offseTable.get(mq);
		if (offset != null) {
			return offset;
		}
		return 0;
	}
}
