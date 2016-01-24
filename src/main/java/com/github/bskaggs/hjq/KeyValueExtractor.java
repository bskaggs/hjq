package com.github.bskaggs.hjq;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;

public final class KeyValueExtractor {
	private final ObjectMapper objectMapper = new ObjectMapper();
	private final Text key = new Text();
	private final Text value = new Text();
	
	public void extract(String json) throws IOException {
		@SuppressWarnings("unchecked")
		Map<String, ?> combined = objectMapper.readValue(json, Map.class);
		
		Object keyObject = combined.get("key");
		if (keyObject == null && !combined.containsKey("key")) {
			throw new IOException("JSON object doesn't have a \"key\" field.");
		}
		String keyString = objectMapper.writeValueAsString(keyObject);
		key.set(keyString);
		
		Object valueObject = combined.get("value");
		if (valueObject == null && !combined.containsKey("value")) {
			throw new IOException("JSON object doesn't have a \"value\" field.");
		}
		String valueString = objectMapper.writeValueAsString(valueObject);
		value.set(valueString);
	}
	
	public Text getKey() {
		return key;
	}
	
	public Text getValue() {
		return value;
	}
	
}
