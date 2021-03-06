package com.alibaba.middleware.race.Tair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.PaySortBolt;
import com.esotericsoftware.minlog.Log;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/ group
 * 、namespace我们都会在正式提交代码前告知选手
 */
public class TairImpl {
	private static Logger LOG = LoggerFactory.getLogger(TairImpl.class);
	private static DefaultTairManager tairManager;
	public static double PCSUM = 0, MOBILESUM = 0;
	// public static File file;
	// public static int failTimes = 0;
	// public static long start;
	static {

		// 创建config server列表
		List<String> confServers = new ArrayList<String>();
		confServers.add(RaceConfig.TairConfigServer);
		confServers.add(RaceConfig.TairSalveConfigServer);
		// 创建客户端实例
		tairManager = new DefaultTairManager();
		tairManager.setConfigServerList(confServers);
		// 设置组名
		tairManager.setGroupName(RaceConfig.TairGroup);
		// 初始化客户端
		tairManager.init();
		// file = new File("/root/result.txt");
	}

	public static boolean writeAll(int index, long millisTime) {
		writeTaobao(index, millisTime);
		writeTmall(index, millisTime);
		wirteRatio(index, millisTime);
		return true;
	}

	/**
	 * tair 写入
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public static boolean write(Serializable key, Serializable value) {
		ResultCode rc = tairManager.put(RaceConfig.TairNamespace, key, value);
		if (rc.isSuccess()) {
			return true;
		} else if (ResultCode.VERERROR.equals(rc)) {
			// 版本错误的处理代码
			Log.error("****************tair版本错误****************");
			return false;
		} else {
			// 其他失败的处理代码
			Log.error("****************tair write失败****************");
			return false;
		}
	}

	/**
	 * tair 读取
	 *
	 * @param key
	 * @return
	 */
	public static Object get(Serializable key) {
		Result<DataEntry> result = tairManager.get(RaceConfig.TairNamespace, key);
		if (result.isSuccess()) {
			DataEntry entry = result.getValue();
			if (entry != null) {
				return entry.getValue();
			} else {
				// 数据不存在
				Log.info("****************数据不存在****************");
				return null;
			}
		} else {
			// 异常处理
			Log.error("****************tair get异常****************");
			return null;
		}

	}

	private static boolean wirteRatio(int index, long millisTime) {
		PCSUM += PaySortBolt.PCDeal[index];
		MOBILESUM += PaySortBolt.mobileDeal[index];
		double result = MOBILESUM / PCSUM;
		// LOG.info((System.currentTimeMillis() - start) + "ms " + index +
		// "@@@@@@@@@@@@@@@@@@@" + RaceConfig.prex_ratio
		// + millisTime + "----->" + result);
		// try {
		// RandomAccessFile raf = new RandomAccessFile(file, "rw");
		// raf.seek(raf.length());
		// raf.write(((System.currentTimeMillis() - start) + "ms " + index + "
		// -> key: " + RaceConfig.prex_ratio
		// + millisTime + " value: " + result + "\n").getBytes());
		// raf.close();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		return write(RaceConfig.prex_ratio + millisTime, result);
	}

	private static boolean writeTaobao(int index, long millisTime) {

		double result = PaySortBolt.taobaoDeal[index];
		// LOG.info(index + "@@@@@@@@@@@@@@@@@@@" + RaceConfig.prex_taobao +
		// millisTime + "----->" + result);
		// try {
		// RandomAccessFile raf = new RandomAccessFile(file, "rw");
		// raf.seek(raf.length());
		// raf.write((index + " -> key: " + RaceConfig.prex_taobao + millisTime
		// + " value: " + result + "\n")
		// .getBytes());
		// raf.close();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		return write(RaceConfig.prex_taobao + millisTime, result);
	}

	private static boolean writeTmall(int index, long millisTime) {
		double result = PaySortBolt.tmallDeal[index];
		// LOG.info(index + "@@@@@@@@@@@@@@@@@@@" + RaceConfig.prex_tmall +
		// millisTime + "----->" + result);
		// try {
		// RandomAccessFile raf = new RandomAccessFile(file, "rw");
		// raf.seek(raf.length());
		// raf.write((index + " -> key: " + RaceConfig.prex_tmall + millisTime +
		// " value: " + result + "\n")
		// .getBytes());
		// raf.close();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		return write(RaceConfig.prex_tmall + millisTime, result);
	}

}
