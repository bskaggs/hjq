package com.github.bskaggs.hjq;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.github.bskaggs.jjq.JJQ;
import com.github.bskaggs.jjq.JJQConsumer;
import com.github.bskaggs.jjq.JJQException;

public class HJQKeyMapper extends HJQAbstractMapper<Text, NullWritable> {


	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		super.setup(context);
		
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
	

}
