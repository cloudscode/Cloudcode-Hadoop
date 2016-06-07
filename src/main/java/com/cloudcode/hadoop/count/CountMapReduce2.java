/**
 * Project Name:Cloudcode-Hadoop
 * File Name:CountMapReduce.java
 * Package Name:com.cloudcode.hadoop.count
 * Date:2016-6-7下午12:42:23
 * Copyright (c) 2016, chenzhou1025@126.com All Rights Reserved.
 *
 */

package com.cloudcode.hadoop.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudcode.hadoop.count.CountMapReduce.CountMapper;
import com.cloudcode.hadoop.count.CountMapReduce.CountReduce;

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
public class CountMapReduce2 extends Configured  implements Tool {
	public static class CountMapper extends Mapper<LongWritable, Text, IntWritable, UserWritable> {
		private UserWritable userWritable = new UserWritable();
		private IntWritable id = new IntWritable();

		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, UserWritable>.Context context)
						throws java.io.IOException, InterruptedException {
			// 获得输入的每行数据
			String line = value.toString();
			String[] words = line.split("\t");
			if (words.length == 3) {
				userWritable.setId(Integer.parseInt(words[0])).setIncome(Integer.parseInt(words[1]))
						.setExpenses(Integer.parseInt(words[2]))
						.setSum(Integer.parseInt(words[1]) - Integer.parseInt(words[2]));
				id.set(Integer.parseInt(words[0]));
			}
			context.write(id, userWritable);
		};
	}

	public static class CountReduce extends Reducer<IntWritable, UserWritable, UserWritable, NullWritable> {
		private UserWritable userWritable = new UserWritable();
		private NullWritable n = NullWritable.get();
		
		protected void reduce(IntWritable key, java.lang.Iterable<UserWritable> values,
				org.apache.hadoop.mapreduce.Reducer<IntWritable, UserWritable, UserWritable, 
				NullWritable>.Context context)
						throws java.io.IOException, InterruptedException {

			Integer income = 0;
			Integer expenses = 0;

			for (UserWritable u : values) {
				income += u.getIncome();
				expenses += u.getExpenses();
			}

			Integer sum = income - expenses;
			userWritable.setId(key.get())
			.setIncome(income)
			.setExpenses(expenses)
			.setSum(sum);
			//输出数据到上下文中
			context.write(userWritable, n);
		};
	}
	public static void main(String[] args) throws Exception {
//		//初始化配置对象
//		Configuration conf = new Configuration();
//		//设置job对象的运行信息和名称
//		Job job =Job.getInstance(conf,"countMR");
//		//设置job运行的类
//		job.setJarByClass(CountMapReduce2.class);
//		//设置mapper输入输出类型及mapper类
//		job.setMapperClass(CountMapper.class);
//		job.setMapOutputKeyClass(IntWritable.class);
//		job.setMapOutputValueClass(UserWritable.class);
//		//设置reduce输出类型及reducer类
//		job.setReducerClass(CountReduce.class);
//		job.setOutputKeyClass(UserWritable.class);
//		job.setOutputValueClass(NullWritable.class);
//		
//		//设置输入输出的路径
//		args = new String[]{
//				"hdfs://master:8020/user_data",
//				"hdfs://master:8020/user_out"
//		};
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		boolean status = job.waitForCompletion(true);
//		if(!status){
//			System.out.println("***************任务失败**********");
//		}
		int status = ToolRunner.run(new CountMapReduce2(), args);
//		ToolRunner.run(conf, tool, args);
		if(status !=1){
			System.out.println("***************任务失败**********");
		}
	}
//	private Configuration conf;
//	public Configuration getConf() {
//	
//		return conf;
//	}
//	public void setConf(Configuration conf) {
//	
//		this.conf =conf;
//	}
	public int run(String[] args) throws Exception {
		//初始化配置对象
				Configuration conf = new Configuration();
				conf.setBoolean("mapreduce.job.ubertask.enable", true);
				//设置job对象的运行信息和名称
				Job job =Job.getInstance(conf,"countMR");
				//设置job运行的类
				job.setJarByClass(CountMapReduce.class);
				//设置mapper输入输出类型及mapper类
				job.setMapperClass(CountMapper.class);
				job.setMapOutputKeyClass(IntWritable.class);
				job.setMapOutputValueClass(UserWritable.class);
				//设置reduce输出类型及reducer类
				job.setReducerClass(CountReduce.class);
				job.setOutputKeyClass(UserWritable.class);
				job.setOutputValueClass(NullWritable.class);
				
				//设置输入输出的路径
				args = new String[]{
						"hdfs://master:8020/user_data",
						"hdfs://master:8020/user_out"
				};
				FileInputFormat.setInputPaths(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				boolean status = job.waitForCompletion(true);
				if(!status){
					System.out.println("***************任务失败**********");
				}
		return status?1:-1;
	}
}
