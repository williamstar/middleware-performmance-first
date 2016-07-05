package com.alibaba.middleware.race;

import java.sql.Date;
import java.text.SimpleDateFormat;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class RaceUtils {

    public static byte[] writeKryoObject(Object object) {
        Output output = new Output(1024);
        Kryo kryo = new Kryo();
        kryo.writeObject(output, object);
        output.flush();
        output.close();
        byte [] ret = output.toBytes();
        output.clear();
        return ret;
    }

    public static <T> T readKryoObject(Class<T> tClass, byte[] bytes) {
        Kryo kryo = new Kryo();
        Input input = new Input(bytes);
        input.close();
        T ret = kryo.readObject(input, tClass);
        return ret;
    }
    
    
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
