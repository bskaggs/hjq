package com.github.bskaggs.hjq;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.ObjectMapper;

import com.github.bskaggs.jjq.JJQ;
import com.github.bskaggs.jjq.JJQConsumer;
import com.github.bskaggs.jjq.JJQException;

public class HJQReducer extends Reducer<Text, Text, Text, NullWritable> {
	public final static String REDUCER_PROGRAM = "hjq.reducer.program";
	public static void setReducerProgram(Job job, String program) {
		job.getConfiguration().set(REDUCER_PROGRAM, program);
	}
	
	private JJQ jjq;
	private ObjectMapper objectMapper = new ObjectMapper();

	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		super.setup(context);
		String program = context.getConfiguration().get(REDUCER_PROGRAM, ".");
		final JJQConsumer consumer = new JJQConsumer() {
			@Override
			public void accept(String json) {
				try {
					context.write(new Text(json), NullWritable.get());
				} catch (IOException e) {
				} catch (InterruptedException e) {
				}
			}
		};
		try {
			jjq = new JJQ(program, consumer);
			
		} catch (JJQException e) {
			throw new IOException(e);
		}
	}
	

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)	throws IOException, InterruptedException {
		try {
			Map<String, Object> object = new LinkedHashMap<String, Object>();
			object.put("key", objectMapper.readValue(key.toString(), Object.class));
			jjq.add("[");
			boolean first = true;
			for (Text value : values) {
				object.put("value", objectMapper.readValue(value.toString(), Object.class));
				
				if (first) {
					jjq.add(objectMapper.writeValueAsString(object));
					first = false;
				} else{
					jjq.add("," + objectMapper.writeValueAsString(object));
				}
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
