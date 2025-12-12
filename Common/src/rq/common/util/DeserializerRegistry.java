package rq.common.util;

import java.util.HashMap;
import java.util.Map;

public class DeserializerRegistry {

	private static final Map<Class<?>, IDeserializer<?>> registry = new HashMap<>();

    public static <T> void register(Class<T> clazz, IDeserializer<T> deserializer) {
        registry.put(clazz, deserializer);
    }

    public static <T> T deserialize(Class<T> clazz, String s) {
    	IDeserializer<T> deserializer = (IDeserializer<T>) registry.get(clazz);
        if (deserializer == null) throw new IllegalArgumentException("No deserializer for " + clazz);
        return deserializer.deserialize(s);
    }
}
