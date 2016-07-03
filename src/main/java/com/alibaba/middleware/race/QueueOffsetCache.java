package com.alibaba.middleware.race;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.rocketmq.common.message.MessageQueue;

public class QueueOffsetCache {
	
	 private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();
	 
    public static void putMessageQueueOffset(MessageQueue mq, long offset) {
    	offseTable.put(mq, offset);
    }

    public static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = offseTable.get(mq);
        if (offset != null)
            return offset;
        return 0;
    }

}
