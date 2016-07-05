package com.alibaba.middleware.race.model;

import java.io.Serializable;

public class SimplePay implements Serializable {
	
	public SimplePay() {
		super();
	}
	public SimplePay( double payAmount, int index) {
		super();
		this.payAmount = payAmount;
		this.index = index;
	}
	public double payAmount; // 金额
	public int index;
}
