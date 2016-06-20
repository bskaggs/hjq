package com.github.bskaggs.hjq;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.github.bskaggs.jjq.JJQ;
import com.github.bskaggs.jjq.JJQConsumer;
import com.github.bskaggs.jjq.JJQException;

public class HJQKeyMapper<KEYIN, VALUEIN> extends HJQAbstractMapper<KEYIN, VALUEIN, Text, NullWritable> {
	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		final JJQConsumer consumer = new JJQConsumer() {
			@Override
			public void accept(String json) throws IOException {
				try {
					context.write(new Text(json), NullWritable.get());
				} catch (InterruptedException e) {
					throw new IOException(e);
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
