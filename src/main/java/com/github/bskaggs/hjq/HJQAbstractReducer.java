package com.github.bskaggs.hjq;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.ObjectMapper;

import com.github.bskaggs.jjq.JJQ;
import com.github.bskaggs.jjq.JJQConsumer;
import com.github.bskaggs.jjq.JJQException;

public abstract class HJQAbstractReducer<K, V> extends Reducer<Text, Text, K, V> {
	public final static String REDUCER_PROGRAM = "hjq.reducer.program";
	
	public static void setReducerProgram(Job job, String program) {
		job.getConfiguration().set(REDUCER_PROGRAM, program);
	}

	protected String getReducerProgram(Configuration conf) {
		return conf.get(REDUCER_PROGRAM, ".");
	}

	private JJQ jjq;
	private final ObjectMapper objectMapper = new ObjectMapper();
	
	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		super.setup(context);
		String program = getReducerProgram(context.getConfiguration());
		final JJQConsumer consumer = newConsumer(context);
		try {
			jjq = new JJQ(program, consumer);
		} catch (JJQException e) {
			throw new IOException(e);
		}
	}

	protected abstract JJQConsumer newConsumer(Context context);

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		try {
			Map<String, Object> object = new LinkedHashMap<String, Object>();
			object.put("key", objectMapper.readValue(key.toString(), Object.class));
			jjq.add("[");
			boolean first = true;
			for (Text value : values) {
				object.put("value", objectMapper.readValue(value.toString(), Object.class));

				if (first) {
					first = false;
				} else {
					jjq.add(",");
				}
				jjq.add(objectMapper.writeValueAsString(object));
			}
			jjq.add("] ");
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
