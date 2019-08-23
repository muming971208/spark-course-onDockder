package org.apache.hadoop.example;

import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InternetCount {
	private final static String INPUT_PATH = "hdfs://localhost:9000/muming/bigdata/final_user.csv";
	private final static String OUTPUT_PATH = "hdfs://localhost:9000/muming/bigdata/result2";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "internet count");
		job.setJarByClass(InternetCount.class);
		job.setMapperClass(InternetCount.TokenizerMapper.class);
		job.setCombinerClass(InternetCount.IntSumReducer.class);
		job.setReducerClass(InternetCount.IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = value.toString().split(",");
			context.write(new Text(data[0]), one);
		}

	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = StreamSupport.stream(values.spliterator(), true).mapToInt(op -> op % 2).sum();
			result.set(sum);
			context.write(key, result);

		}
	}
}
