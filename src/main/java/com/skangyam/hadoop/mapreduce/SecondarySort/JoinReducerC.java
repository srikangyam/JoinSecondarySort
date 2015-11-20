package com.skangyam.hadoop.mapreduce.SecondarySort;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducerC extends Reducer<CompositeKey, Text, Text, Text> {
	@Override
	protected void reduce(CompositeKey key, Iterable<Text> values, Context context)
	          throws IOException,InterruptedException{
		HashSet<String> emp = new HashSet<String>();
		HashSet<String> dep = new HashSet<String>();
		
		for (Text info : values){
			String str = info.toString();
			if (str.contains(",")){
				emp.add(str);
			}
			else{
				dep.add(str);
			}
		}
		
		Iterator<String> itrD = dep.iterator();
		Iterator<String> itrE = emp.iterator();
		
		while (itrD.hasNext()){
			String dept = itrD.next();
			while (itrE.hasNext()){
				String empt = itrE.next();
				String row = key.getId() + "," + dept + "," + empt;
				context.write(new Text(row), new Text());
			}
		}
	}

}
