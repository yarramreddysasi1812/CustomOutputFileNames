package com.weather.poc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<Object, Text, Text, FloatWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text dayReport, Context context)
			throws IOException, InterruptedException {
		StringTokenizer st2 = new StringTokenizer(dayReport.toString(), "\t");

		int counter = 0;
		String cityDateString = "";
		String maxTempTime = "";
		String minTempTime = "";
		String curTime = "";
		float curTemp = 0;
		float minTemp = Float.MAX_VALUE;
		float maxTemp = Float.MIN_VALUE;

		while (st2.hasMoreElements()) {
			if (counter == 0) {
				cityDateString = st2.nextToken();
			} else {
				if (counter % 2 == 1) {
					curTime = st2.nextToken();
				} else if (counter % 2 == 0) {
					curTemp = Float.parseFloat(st2.nextToken());
					if (minTemp > curTemp) {
						minTemp = curTemp;
						minTempTime = curTime;
					} else if (maxTemp < curTemp) {
						maxTemp = curTemp;
						maxTempTime = curTime;
					}
				}
			}
			counter++;
		}

		FloatWritable fValue = new FloatWritable();
		Text cityDate = new Text();

		fValue.set(maxTemp);
		cityDate.set(cityDateString);
		context.write(cityDate, fValue);

		fValue.set(minTemp);
		cityDate.set(cityDateString);
		context.write(cityDate, fValue);
	}
}
