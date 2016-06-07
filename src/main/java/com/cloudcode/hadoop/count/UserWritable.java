/**
 * Project Name:Cloudcode-Hadoop
 * File Name:UserWritable.java
 * Package Name:com.cloudcode.hadoop.count
 * Date:2016-6-7下午12:44:55
 * Copyright (c) 2016, chenzhou1025@126.com All Rights Reserved.
 *
 */

package com.cloudcode.hadoop.count;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * ClassName:UserWritable <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016-6-7 下午12:44:55 <br/>
 * 
 * @author cloudscode ljzhuanjiao@Gmail.com
 * @version
 * @since JDK 1.6
 * @see
 */
public class UserWritable implements WritableComparable<UserWritable> {

	private Integer id;
	private Integer income;
	private Integer expenses;
	private Integer sum;

	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.income = in.readInt();
		this.expenses = in.readInt();
		this.sum = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(income);
		out.writeInt(expenses);
		out.writeInt(sum);
	}

	public int compareTo(UserWritable o) {

		return this.id > o.getId() ? 1 : -1;
	}

	public Integer getId() {
		return id;
	}

	public UserWritable setId(Integer id) {
		this.id = id;
		return this;
	}

	public Integer getIncome() {
		return income;
	}

	public UserWritable setIncome(Integer income) {
		this.income = income;
		return this;
	}

	public Integer getExpenses() {
		return expenses;
	}

	public UserWritable setExpenses(Integer expenses) {
		this.expenses = expenses;
		return this;
	}

	public Integer getSum() {
		return sum;
	}

	public UserWritable setSum(Integer sum) {
		this.sum = sum;
		return this;
	}

	@Override
	public String toString() {
		return id+"\t"+income+"\t"+expenses+"\t"+sum;
		
	}
}
