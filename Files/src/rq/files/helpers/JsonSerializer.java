package rq.files.helpers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerializer {
	private ObjectMapper objectMapper = null;
	
	private ObjectMapper objectMapper() {
		if(this.objectMapper == null) {
			this.objectMapper = new ObjectMapper();
		}
		return this.objectMapper;
	}
	
	public String serialize(Object obj) {
		try {
			return this.objectMapper().writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
	
	public<T> T deserialize(String json, Class<T> clazz){
		try {
			return this.objectMapper().readValue(json, clazz);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
	
	private static JsonSerializer singleton = null;
	public static JsonSerializer instance() {
		if(singleton == null) {
			singleton = new JsonSerializer();
		}
		return singleton;
	}
}
