package rq.common.util;

public class NumberDeserializer {

	@SuppressWarnings("unchecked")
	public static <T extends Number> T deserialize(String s, Class<T> type) {
	    if (type == Integer.class) return (T) Integer.valueOf(s);
	    if (type == Long.class)    return (T) Long.valueOf(s);
	    if (type == Short.class)   return (T) Short.valueOf(s);
	    if (type == Byte.class)    return (T) Byte.valueOf(s);
	    if (type == Float.class)   return (T) Float.valueOf(s);
	    if (type == Double.class)  return (T) Double.valueOf(s);

	    throw new IllegalArgumentException("Unsupported number type: " + type);
	}

}
