package com.skangyam.hadoop.mapreduce.SecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperEmpC extends Mapper<LongWritable, Text, CompositeKey, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CompositeKey, Text>.Context context)
	          throws IOException, InterruptedException{
		String[] str = value.toString().split(",");
		CompositeKey empKey = new CompositeKey(str[2], "1");
		context.write(empKey, new Text(str[0]+","+str[1]));
	}

}
