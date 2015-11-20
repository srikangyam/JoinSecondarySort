package com.skangyam.hadoop.mapreduce.SecondarySort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey, Text> {
    HashPartitioner<Text, Text> hashP = new HashPartitioner<Text,Text>();
    Text newKey = new Text();

	@Override
	public int getPartition(CompositeKey key, Text value, int numOfRed) {
		try{
			newKey.set(key.getId());
			return hashP.getPartition(newKey, value, numOfRed);
		}catch (Exception e){
			e.printStackTrace();
			return (int) (Math.random() * numOfRed);
		}
	}
}
