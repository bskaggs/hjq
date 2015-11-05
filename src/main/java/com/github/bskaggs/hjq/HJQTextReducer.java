package com.github.bskaggs.hjq;

import org.apache.hadoop.io.Text;

public class HJQTextReducer extends HJQAbstractReducer<Text> {

	@Override
	protected Text encode(String json) {
		return new Text(json);
	}
	
}
