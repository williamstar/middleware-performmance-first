package com.alibaba.middleware.race.jstorm.bolt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.Tair.TairImpl;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.model.SimplePay;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * PC/无线、淘宝/天猫的分类、提交通知
 */
public class PaySortBolt implements IRichBolt, Serializable {
	private static Logger LOG = LoggerFactory.getLogger(PaySortBolt.class);
	private static boolean isStart = true;
	private static boolean isOffer = false;
	// 缓存结构 orderid : payAmount
	public static HashMap<Long, Double> taobaoCacheMap = new HashMap<>(300000);
	public static HashMap<Long, Double> tmallCacheMap = new HashMap<>(300000);
	public static HashMap<Long, ArrayList<SimplePay>> payCacheMap = new HashMap<>(300000);

	// 结果记录
	public static double[] taobaoDeal = new double[1441];
	public static double[] tmallDeal = new double[1440];
	public static double[] PCDeal = new double[1440];
	public static double[] mobileDeal = new double[1440];
	public static long[] timeStamp = new long[1440];// 10位
	
	@Override
	public void execute(Tuple input) {
		int flag = (int) input.getValue(2);

		if (flag == 2) {
			// pay
			PaymentMessage payment = (PaymentMessage) input.getValue(0);
			int index = (int) input.getValue(1);
			long minuteTime = (payment.createTime / 1000 / 60) * 60;
			if (isStart == true) {
				taobaoDeal[1440] = index;
				isStart = false;
			}
			timeStamp[index] = minuteTime;// 下标-10位时间戳映射
			// 0 PC、1 无线
			if (payment.payPlatform == 0) {
				PCDeal[index] += payment.payAmount;
			} else {
				mobileDeal[index] += payment.payAmount;
			}
			if (taobaoCacheMap.containsKey(payment.orderId)) {
				taobaoDeal[index] += payment.payAmount;
				// 更新订单总金额,如果全消费完则删除订单
				double sub = taobaoCacheMap.get(payment.orderId) - payment.payAmount;
				taobaoCacheMap.put(payment.orderId, sub);
				if (sub < 0.01) {
					taobaoCacheMap.remove(payment.orderId);
				}
			} else if (tmallCacheMap.containsKey(payment.orderId)) {
				tmallDeal[index] += payment.payAmount;
				// 更新订单总金额,如果全消费完则删除订单
				double sub = tmallCacheMap.get(payment.orderId) - payment.payAmount;
				tmallCacheMap.put(payment.orderId, sub);
				if (sub < 0.01) {
					tmallCacheMap.remove(payment.orderId);
				}
			} else {
				// faster than order!
				ArrayList<SimplePay> cacheList = payCacheMap.get(payment.orderId);
				if (cacheList == null) {
					cacheList = new ArrayList<>();
					cacheList.add(new SimplePay(payment.payAmount, index));
					payCacheMap.put(payment.orderId, cacheList);
				} else {
					cacheList.add(new SimplePay(payment.payAmount, index));
				}

//				System.err.println("      Current TaobaoMap、TmallMap size is " + taobaoCacheMap.size() + "  ,  "
//						+ tmallCacheMap.size() + "	===========pay is faster than order	!!!============");
			}
		} else if (flag == 0) {
			// taobao
			long id = (long) input.getValue(0);
			double amount = (double) input.getValue(1);
			ArrayList<SimplePay> cacheList = payCacheMap.get(id);
			if (cacheList != null) {
				for (SimplePay pay : cacheList) {
					taobaoDeal[pay.index] += pay.payAmount;
					amount -= pay.payAmount;
				}
				if (amount > 0.01) {
					taobaoCacheMap.put(id, amount);
				}
				payCacheMap.remove(id);
			} else {
				taobaoCacheMap.put(id, amount);
			}
		} else if (flag == 1) {
			// tmall
			long id = (long) input.getValue(0);
			double amount = (double) input.getValue(1);
			ArrayList<SimplePay> cacheList = payCacheMap.get(id);
			if (cacheList != null) {
				for (SimplePay pay : cacheList) {
					tmallDeal[pay.index] += pay.payAmount;
					amount -= pay.payAmount;
				}
				if (amount > 0.01) {
					tmallCacheMap.put(id, amount);
				}
				payCacheMap.remove(id);
			} else {
				tmallCacheMap.put(id, amount);
			}
		} else if (flag == -1) {
			// end
			isOffer = true;
			submitFinalMsg();
		}

	}

	// 提交全部统计结果
	public static void submitFinalMsg() {
//		LOG.info("END~~~~~~      Current TaobaoMap、TmallMap size is " + taobaoCacheMap.size() + "  ,  "
//				+ tmallCacheMap.size());

		TairImpl.PCSUM = TairImpl.MOBILESUM = 0d;
		for (int i = (int) taobaoDeal[1440]; i < 1440; i++) {
			if (taobaoDeal[i] != 0d) {
				TairImpl.writeAll(i, timeStamp[i]);
			}
		}
		for (int i = 0; i < (int) taobaoDeal[1440]; i++) {
			if (taobaoDeal[i] != 0d) {
				TairImpl.writeAll(i, timeStamp[i]);
			}
		}

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

	// 19:55 emit 
	public static void timeEmit() {
		new Timer().schedule(new TimerTask() {
			@Override
			public void run() {
				if(isOffer = false){
					submitFinalMsg();
				}
			}
		},1195000l);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		TairImpl.start = System.currentTimeMillis();
		timeEmit();
	}

}
