package rq.files.helpers;

import rq.common.io.contexts.ClassNotInContextException;
import rq.common.io.contexts.ValueSerializerContext;

/**
 * Serializes a single value
 * @author Mgr. R.Skrabal
 *
 */
public class ValueSerializer {
	private final Object serialized;
	private final ValueSerializerContext context;
	
	public ValueSerializer(Object serialized, ValueSerializerContext context) {
		this.serialized = serialized;
		this.context = context;
	}
	
	/**
	 * Serializes the value
	 * @return serialized value
	 * @throws ClassNotInContextException
	 */
	public String serialize() throws ClassNotInContextException {
		String s = this.context.serializeValue(this.serialized);
		return s;
	}
	
	/**
	 * Serializes the value from arguments
	 * @param attribute
	 * @param serialized
	 * @param context
	 * @return
	 * @throws ClassNotInContextException
	 */
	public static String serialize(Object serialized, ValueSerializerContext context) 
			throws ClassNotInContextException {
		ValueSerializer serializer = new ValueSerializer(serialized, context);
		String s = serializer.serialize();
		return s;
	}
	
	/**
	 * Serializes the value from arguments
	 * @param attribute
	 * @param serialized
	 * @return
	 * @throws ClassNotInContextException
	 */
	public static String serialize(Object serialized) 
			throws ClassNotInContextException {
		return ValueSerializer.serialize(serialized, ValueSerializerContext.DEFAULT);
	}
}
