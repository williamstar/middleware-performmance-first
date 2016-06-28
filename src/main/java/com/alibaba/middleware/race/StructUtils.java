package com.alibaba.middleware.race;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.concurrent.ConcurrentHashMap;

public class StructUtils {
	//50W:100000,60000
	public static final int UPPER_BOUNDER =150000;
	public static final int LOWER_BOUNDER = 100000;
	//缓存结构	orderid : payAmount
	public static ConcurrentHashMap<Long,Double> taobaoCacheMap= new ConcurrentHashMap<Long,Double>();
	public static ConcurrentHashMap<Long,Double> tmallCacheMap = new ConcurrentHashMap<Long,Double>();

	//结果记录
	public static double[] taobaoDeal = new double[1441];
	public static double[] tmallDeal = new double[1440];
	public static double[] PCDeal = new double[1440];
	public static double[] mobileDeal = new double[1440];
	public static long[] timeStamp = new long[1440];//10位
	
	/**
	 * 将13位时间戳转化成该分钟对应的index(24h*60min)
	 * @param strStamp
	 * @return	index [0,1440)
	 */
	public static int parseTimeStamp2Int(long lStamp){
		Date date = new Date(lStamp);
		SimpleDateFormat sdf = new SimpleDateFormat();
		sdf.applyPattern("HH");
		int hour = Integer.parseInt(sdf.format(date));
		sdf.applyPattern("mm");
		int minute = Integer.parseInt(sdf.format(date));
		return hour*60+minute;
	}

}
