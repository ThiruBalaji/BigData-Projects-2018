package org.edureka.project.complaints;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

/*Finding the list of people with particular grade who have taken loan */

public class Usecase1 {

	public static class Map extends Mapper<LongWritable,Text,Text,Text> {
		
		
		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException,InterruptedException
		{	
			
			String line=value.toString();
			
			/*Considering Term as one year and finding the people whose interest is mare that 5,000*/		
			String parts[]=line.split(",");
			String complaint_id=parts[0];
			String product=parts[1];
			
			
			context.write(new Text(product), new Text(complaint_id));
			
			
		}
	}
	
	public static class Reduce extends	Reducer<Text, Text, Text, IntWritable> {

   	
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
    	  int count=0;
    	  for(Text record:values){
    		  count++;
    	  }
    	  context.write(key, new IntWritable(count));
    	  
		}
	}
	
	
	public static void main(String[] args) throws Exception { 
		
		Configuration conf= new Configuration();
		
		Job job = new Job(conf,"LoanUseCase2");
    
		
		//Defining the output value class for the mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(Usecase1.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//Defining the output value class for the mapper
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
			
		Path outputPath = new Path(args[1]);
		
		FileSystem s=outputPath.getFileSystem(conf);
		if(s.isDirectory(outputPath)){
			s.delete(outputPath);
		}
		
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, outputPath);
	    
			//deleting the output path automatically from hdfs so that we don't have delete it explicitly
			
		outputPath.getFileSystem(conf).delete(outputPath);
		
			
			//exiting the job only if the flag value becomes false
			
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

	}

