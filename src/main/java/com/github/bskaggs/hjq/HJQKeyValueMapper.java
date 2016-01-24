package com.github.bskaggs.hjq;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.github.bskaggs.jjq.JJQ;
import com.github.bskaggs.jjq.JJQConsumer;
import com.github.bskaggs.jjq.JJQException;

public class HJQKeyValueMapper<KEYIN, VALUEIN> extends HJQAbstractMapper<KEYIN, VALUEIN, Text, Text> {
	private KeyValueExtractor kvExtractor = new KeyValueExtractor();

	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		super.setup(context);

		final JJQConsumer consumer = new JJQConsumer() {
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

		try {
			jjq = new JJQ(program, consumer);
		} catch (JJQException e) {
			throw new IOException(e);
		}
	}
}
