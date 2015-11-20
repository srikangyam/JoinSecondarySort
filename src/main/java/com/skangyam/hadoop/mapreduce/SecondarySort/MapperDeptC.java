package com.skangyam.hadoop.mapreduce.SecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperDeptC extends Mapper<LongWritable, Text, CompositeKey, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CompositeKey, Text>.Context context)
	          throws IOException,InterruptedException{
		String[] str = value.toString().split(",");
		CompositeKey deptKey = new CompositeKey(str[0],"0");
		context.write(deptKey, new Text(str[1]));
	}

}
