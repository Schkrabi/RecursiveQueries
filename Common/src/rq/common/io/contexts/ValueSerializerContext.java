package rq.common.io.contexts;

import java.util.function.Function;


import java.util.Map;
import java.util.HashMap;
import java.time.LocalDateTime;

/**
 * Context for the table serialization
 * @author Mgr. R.Skrabal
 *
 */
public class ValueSerializerContext {
	private Map<Class<?>, Function<Object, String>> serializers;
	
	public ValueSerializerContext(Map<Class<?>, Function<Object, String>> serializers) {
		this.serializers = new HashMap<Class<?>, Function<Object, String>>(serializers);
	}
	
	/**
	 * Serialize value 
	 * @param type
	 * @param object
	 * @return string with serialized value
	 * @throws ClassNotInContextException
	 */
	public String serializeValue(Object object) 
		throws ClassNotInContextException {
		Function<Object, String> sFunction = this.getSerializingFunction(object.getClass());
		String s = sFunction.apply(object);
		return s;
	}
	
	/**
	 * Gets the function for serialization a specific type
	 * @param type
	 * @return Serialization function
	 * @throws ClassNotInContextException
	 */
	public Function<Object, String> getSerializingFunction(Class<?> type)
		throws ClassNotInContextException {
		Function<Object, String> sFunction = this.serializers.get(type);
		if(type == null) {
			throw new ClassNotInContextException(this, type);
		}
		return sFunction;
	}
	
	/**
	 * Default serialization context
	 */
	public static final ValueSerializerContext DEFAULT =
			new ValueSerializerContext(Map.of(
					String.class, (Object o) -> o.toString(),
					Integer.class, (Object o) -> o.toString(),
					Double.class, (Object o) -> o.toString(),
					Float.class, (Object o) -> o.toString(),
					Boolean.class, (Object o) -> o.toString(),
					LocalDateTime.class, (Object o) -> ((LocalDateTime)o).format(DateTimeFormatterProvider.formatter()),
					rq.common.types.Str10.class, (Object o) -> o.toString(),
					rq.common.types.Str50.class, (Object o) -> o.toString(),
					rq.common.types.DateTime.class, (Object o) -> ((rq.common.types.DateTime)o).getInner().format(DateTimeFormatterProvider.formatter())
					));
}
