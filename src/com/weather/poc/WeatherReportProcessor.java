package com.weather.poc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WeatherReportProcessor {

	public static String caOutputName = "California";
	public static String nyOutputName = "Newyork";
	public static String njOutputName = "Newjersy";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Weather Report");
		job.setJarByClass(WeatherReportProcessor.class);

		job.setMapperClass(WeatherMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(WeatherReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		job.setOutputKeyClass(Text.class);// <hadoop,4>
		job.setOutputValueClass(Text.class);

		MultipleOutputs.addNamedOutput(job, caOutputName,
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, nyOutputName,
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, njOutputName,
				TextOutputFormat.class, Text.class, Text.class);

		// job.setNumReduceTasks(0);
		// job.setInputFormatClass(KeyValueTextInputFormat)

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

