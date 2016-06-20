package com.github.bskaggs.hjq;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import com.github.bskaggs.jjq.JJQ;
import com.github.bskaggs.jjq.JJQException;

public class HJQAbstractMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
	public final static String MAPPER_PROGRAM = "hjq.mapper.program";
	protected JJQ jjq;
	protected String program;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		program = getMapperProgram(context.getConfiguration());
	}
	
	@Override
	protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
		try {
			jjq.add(key.toString());
			jjq.add("\n");
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

	public static void setMapperProgram(Job job, String mapperProgram) {
		job.getConfiguration().set(MAPPER_PROGRAM, mapperProgram);		
	}
	
	protected String getMapperProgram(Configuration conf) {
		return conf.get(MAPPER_PROGRAM, ".");
	}
}
