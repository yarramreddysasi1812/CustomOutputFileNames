package com.weather.poc;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WeatherReducer extends Reducer<Text, FloatWritable, Text, Text> {// hadoop,1,1,1,

	MultipleOutputs<Text, Text> mos;

	public void setup(Context context) {
		mos = new MultipleOutputs<Text, Text>(context);
	}

	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		int counter = 0;
		float f1 = 0, f2 = 0;
		Text result = new Text();

		for (FloatWritable value : values) {
			if (counter == 0)
				f1 = value.get();
			else
				f2 = value.get();

			counter = counter + 1;
		}
		if (f1 > f2) {
			// context.write(key, new
			// Text(Float.toString(f2)+"\t"+Float.toString(f1)));
			result = new Text(Float.toString(f2) + "\t" + Float.toString(f1));
		} else {
			// context.write(key, new
			// Text(Float.toString(f1)+"\t"+Float.toString(f2)));
			result = new Text(Float.toString(f1) + "\t" + Float.toString(f2));
		}
		String fileName = "";
		if (key.toString().contains("CA")) {
			fileName = WeatherReportProcessor.caOutputName;
		} else if (key.toString().contains("NY")) {
			fileName = WeatherReportProcessor.nyOutputName;
		} else if (key.toString().contains("NJ")) {
			fileName = WeatherReportProcessor.njOutputName;
		}

		String strArr[] = key.toString().split("_");
		key.set(strArr[1]);
		mos.write(fileName, key, result);
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

}
