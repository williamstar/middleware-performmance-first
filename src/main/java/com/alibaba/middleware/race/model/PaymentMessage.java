package com.alibaba.middleware.race.model;

import java.io.Serializable;
import java.util.Random;


/**
 * 我们后台RocketMq存储的交易消息模型类似于PaymentMessage，选手也可以自定义
 * 订单消息模型，只要模型中各个字段的类型和顺序和PaymentMessage一样，即可用Kryo
 * 反序列出消息
 */

public class PaymentMessage implements Serializable{


	public long orderId; //订单ID

    public double payAmount; //金额

    /**
     * Money来源
     * 0,支付宝
     * 1,红包或代金券
     * 2,银联
     * 3,其他
     */
    public short paySource; //来源

    /**
     * 支付平台
     * 0，pC
     * 1，无线
     */
    public short payPlatform; //支付平台

    /**
     * 付款记录创建时间
     */
    public long createTime; //13位数，毫秒级时间戳，初赛要求的时间都是指该时间

    //Kryo默认需要无参数构造函数
    public PaymentMessage() {
    }

    private static Random rand = new Random();

    public static PaymentMessage[] createPayMentMsg(OrderMessage orderMessage) {
        PaymentMessage [] list = new PaymentMessage[2];
        for (short i = 0; i < 2; i++) {
            PaymentMessage msg = new PaymentMessage();
            msg.orderId = orderMessage.orderId;
            msg.paySource = i;
            msg.payPlatform = (short) (i % 2);
            msg.createTime = orderMessage.createTime + rand.nextInt(100);
            msg.payAmount = 0.0;
            list[i] = msg;
        }

        list[0].payAmount = rand.nextInt((int) (orderMessage.totalPrice / 2));
        list[1].payAmount = orderMessage.totalPrice - list[0].payAmount;

        return list;
    }

    @Override
    public String toString() {
        return "PaymentMessage{" +
                "orderId=" + orderId +
                ", payAmount=" + payAmount +
                ", paySource=" + paySource +
                ", payPlatform=" + payPlatform +
                ", createTime=" + createTime +
                '}';
    }

  
}
