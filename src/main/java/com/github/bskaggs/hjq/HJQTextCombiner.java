package com.github.bskaggs.hjq;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.github.bskaggs.jjq.JJQConsumer;

public class HJQTextCombiner extends HJQAbstractReducer<Text, Text> {
	public final static String COMBINER_PROGRAM = "hjq.combiner.program";
	
	public static void setCombinerProgram(Job job, String program) {
		job.getConfiguration().set(COMBINER_PROGRAM, program);
	}

	@Override
	protected String getReducerProgram(Configuration conf) {
		return conf.get(COMBINER_PROGRAM, ".[]");
	}
	
	private final KeyValueExtractor kvExtractor = new KeyValueExtractor();

	@Override
	protected JJQConsumer newConsumer(final Context context) {
		return new JJQConsumer() {
			@Override
			public void accept(String json) throws IOException {
				kvExtractor.extract(json);
				try {
					context.write(kvExtractor.getKey(), kvExtractor.getValue());
				} catch (InterruptedException e) {
					throw new IOException(e);
				}
			}
		};
	}
}
