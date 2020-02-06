package com.edureka.chainmapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ChainWordCount extends Configured {

	public static class Tokenizer extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			System.out.println("Line:" + line);
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}

	}

	public static class UpperCase extends
			Mapper<Text, IntWritable, Text, IntWritable> {

		public void map(Text key, IntWritable value, Context context)
				throws IOException, InterruptedException {

			String word = key.toString().toUpperCase();
			System.out.println("Upper Case:" + word);
			context.write(new Text(word), value);
		}

	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable x : values) {
				sum += x.get();
			}
			context.write(key, new IntWritable(sum));

		}
	}

	
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "chainmapper");

		job.setJarByClass(ChainWordCount.class);
		
		Configuration tokenizerconf = new Configuration(false);
		ChainMapper.addMapper(job, Tokenizer.class, LongWritable.class, Text.class, Text.class, IntWritable.class, tokenizerconf);
		
		Configuration uppercaseconf = new Configuration(false);
		ChainMapper.addMapper(job, UpperCase.class, Text.class, IntWritable.class, Text.class, IntWritable.class, uppercaseconf);
		
		job.setReducerClass(Reduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
