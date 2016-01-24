package com.github.bskaggs.hjq;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.github.bskaggs.jjq.JJQConsumer;

public class HJQTextReducer extends HJQAbstractReducer<Text, NullWritable> {
	@Override
	protected JJQConsumer newConsumer(final Context context) {
		return new JJQConsumer() {
			@Override
			public void accept(String json) throws IOException {
				try {
					context.write(new Text(json), NullWritable.get());
				} catch (InterruptedException e) {
				}
			}
		};
	}
}
