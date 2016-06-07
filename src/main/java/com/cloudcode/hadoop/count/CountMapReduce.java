/**
 * Project Name:Cloudcode-Hadoop
 * File Name:CountMapReduce.java
 * Package Name:com.cloudcode.hadoop.count
 * Date:2016-6-7下午12:42:23
 * Copyright (c) 2016, chenzhou1025@126.com All Rights Reserved.
 *
 */

package com.cloudcode.hadoop.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ClassName:CountMapReduce <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: 2016-6-7 下午12:42:23 <br/>
 * 
 * @author cloudscode ljzhuanjiao@Gmail.com
 * @version
 * @since JDK 1.6
 * @see
 */
public class CountMapReduce {
	public static class CountMapper extends
			Mapper<LongWritable, Text, IntWritable, UserWritable> {
		private UserWritable userWritable=new UserWritable();
		private IntWritable id = new IntWritable();
		
		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, UserWritable>.Context context)
				throws java.io.IOException, InterruptedException {
			//获得输入的每行数据
			String line = value.toString();
			String[] words = line.split("\t");
			if (words.length == 3) {
				userWritable.setId(Integer.parseInt(words[0]))
				.setIncome(Integer.parseInt(words[1]))
				.setExpenses(Integer.parseInt(words[2]))
				.setSum(Integer.parseInt(words[1]) - Integer.parseInt(words[2]));
				id.set(Integer.parseInt(words[0]));
			}
			context.write(id, userWritable);
		};
	}
	public static class CountReduce extends Reducer<IntWritable, UserWritable, UserWritable, NullWritable>{
		private UserWritable userWritable=new UserWritable();
		protected void reduce(IntWritable arg0, java.lang.Iterable<UserWritable> arg1, org.apache.hadoop.mapreduce.Reducer<IntWritable,UserWritable,UserWritable,NullWritable>.Context arg2) throws java.io.IOException ,InterruptedException {
			
		};
	}
}
