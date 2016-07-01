package com.alibaba.middleware.race.jstorm.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.StructUtils;
import com.alibaba.middleware.race.model.PaymentMessage;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * PC/无线、淘宝/天猫的分类、提交通知
 */
public class PaySortBolt implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(PaySortBolt.class);
	private static OutputCollector collector;
	private static int lastMinFlag = -1; // 上一分钟index

	@Override
	public void execute(Tuple input) {
		PaymentMessage payment = RaceUtils.readKryoObject(PaymentMessage.class, (byte[]) input.getValue(0));
		/********************** 判断是否可以提交 *************************/
		// 一天的整分时刻下标
		final int index = StructUtils.parseTimeStamp2Int(payment.createTime);
		// 整分时刻13位时间戳
		Long minuteTime = (payment.createTime / 1000 / 60) * 60000;
		// 变量初始化
		if (lastMinFlag == -1) {
			StructUtils.taobaoDeal[1440] = index;// 第一个pay下标
			lastMinFlag = index;
		}
		StructUtils.timeStamp[index] = minuteTime / 1000;// 下标-10位时间戳映射
		/********************** 分类 *************************/
		// 0 PC、1 无线
		if (payment.payPlatform == 0) {
			StructUtils.PCDeal[index] += payment.payAmount;
		} else {
			StructUtils.mobileDeal[index] += payment.payAmount;
		}

		// 根据order map判断taobao 或 tmall 订单。
		if (StructUtils.taobaoCacheMap.containsKey(payment.orderId)) {
			StructUtils.taobaoDeal[index] += payment.payAmount;
			// 更新订单总金额,如果全消费完则删除订单
			StructUtils.taobaoCacheMap.put(payment.orderId,
					(Double) StructUtils.taobaoCacheMap.get(payment.orderId) - payment.payAmount);
			if (StructUtils.taobaoCacheMap.get(payment.orderId) < 0.01) {
				StructUtils.taobaoCacheMap.remove(payment.orderId);
			}
		} else if (StructUtils.tmallCacheMap.containsKey(payment.orderId)) {
			StructUtils.tmallDeal[index] += payment.payAmount;
			// 更新订单总金额,如果全消费完则删除订单
			StructUtils.tmallCacheMap.put(payment.orderId,
					(Double) StructUtils.tmallCacheMap.get(payment.orderId) - payment.payAmount);
			if (StructUtils.tmallCacheMap.get(payment.orderId) < 0.01) {
				StructUtils.tmallCacheMap.remove(payment.orderId);
			}
		} else {
			System.err.println("      Current TaobaoMap、TmallMap size is " + StructUtils.taobaoCacheMap.size() + "  ,  "
					+ StructUtils.tmallCacheMap.size() + "	===========pay is faster than order	!!!============");
		}
//		collector.ack(input);
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

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

}
