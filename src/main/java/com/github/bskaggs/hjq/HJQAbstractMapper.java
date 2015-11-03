package com.github.bskaggs.hjq;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.github.bskaggs.jjq.JJQ;
import com.github.bskaggs.jjq.JJQException;

public class HJQAbstractMapper<K,V> extends Mapper<Writable, Text, K, V>{
	protected JJQ jjq;
	protected String program;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		program = context.getConfiguration().get("hjq.mapper.program", ".");
	}
	@Override
	protected void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			jjq.add(value + " ");
		} catch (JJQException e) {
			throw new IOException(e);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		try {
			jjq.finish();
			
		} catch (JJQException e) {
			throw new IOException(e);
		}
	}
}
