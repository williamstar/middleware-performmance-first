package com.alibaba.middleware.race.model;

import java.io.Serializable;

/**
 * 我们后台RocketMq存储的订单消息模型类似于OrderMessage，选手也可以自定义
 * 订单消息模型，只要模型中各个字段的类型和顺序和OrderMessage一样，即可用Kryo
 * 反序列出消息
 */
public class OrderMessage implements Serializable{

	public long orderId; //订单ID
    public String buyerId; //买家ID
    public String productId; //商品ID

    public String salerId; //卖家ID
    public long createTime; //13位数数，毫秒级时间戳，订单创建时间
    public double totalPrice;


    //Kryo默认需要无参数构造函数
    private OrderMessage() {

    }

    public static OrderMessage createTbaoMessage() {
        OrderMessage msg =  new OrderMessage();
        msg.orderId = TableItemFactory.createOrderId();
        msg.buyerId = TableItemFactory.createBuyerId();
        msg.productId = TableItemFactory.createProductId();
        msg.salerId = TableItemFactory.createTbaoSalerId();
        msg.totalPrice = TableItemFactory.createTotalPrice();
        return msg;
    }

    public static OrderMessage createTmallMessage() {
        OrderMessage msg =  new OrderMessage();
        msg.orderId = TableItemFactory.createOrderId();
        msg.buyerId = TableItemFactory.createBuyerId();
        msg.productId = TableItemFactory.createProductId();
        msg.salerId = TableItemFactory.createTmallSalerId();
        msg.totalPrice = TableItemFactory.createTotalPrice();

        return msg;
    }

    @Override
    public String toString() {
        return "OrderMessage{" +
                "orderId=" + orderId +
                ", buyerId='" + buyerId + '\'' +
                ", productId='" + productId + '\'' +
                ", salerId='" + salerId + '\'' +
                ", createTime=" + createTime +
                ", totalPrice=" + totalPrice +
                '}';
    }
}
