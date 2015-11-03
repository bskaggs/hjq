package com.github.bskaggs.hjq;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;

import com.github.bskaggs.jjq.JJQ;
import com.github.bskaggs.jjq.JJQConsumer;
import com.github.bskaggs.jjq.JJQException;

public class HJQKeyValueMapper extends HJQAbstractMapper<Text, Text> {
	
	private ObjectMapper objectMapper = new ObjectMapper();

	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		super.setup(context);
		String program = context.getConfiguration().get("hjq.mapper.program", ".");
		final JJQConsumer consumer = new JJQConsumer() {
			@Override
			public void accept(String json) {
				try {
					Map<String, ?> combined = objectMapper.readValue(json, Map.class);
					String key = objectMapper.writeValueAsString(combined.get("key"));
					String value = objectMapper.writeValueAsString(combined.get("value"));
					context.write(new Text(key), new Text(value));
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
}
