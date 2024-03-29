package rq.files.io;

import java.util.Map;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.function.Function;

import rq.files.exceptions.ClassNotInContextException;
import rq.files.helpers.DateTimeFormatterProvider;

/**
 * Context for parsing values
 * @author Mgr. R.Skrabal
 *
 */
public class ValueParserContext {
	private Map<Class<?>, Function<String, Object>> parsers;
	
	public ValueParserContext(Map<Class<?>, Function<String, Object>> parsers) {
		this.parsers = new HashMap<Class<?>, Function<String, Object>>(parsers);
	}
	
	/**
	 * Parses given value to given class using this context
	 * @param targetClass 
	 * @param value
	 * @return parsed object
	 * @throws ClassNotInContextException if this context does not have the parser for class specified
	 */
	public Object parseValue(Class<?> targetClass, String value) 
		throws ClassNotInContextException {
		Function<String, Object> pFunction = this.getParsingFunction(targetClass);
		Object parsed = pFunction.apply(value);
		return parsed;
		
	}
	
	/**
	 * Gets parsing function for given class in this context
	 * @param targetClass parsed class
	 * @return function
	 * @throws ClassNotInContextException if function does not exists
	 */
	public Function<String, Object> getParsingFunction(Class<?> targetClass)
		throws ClassNotInContextException {
		Function<String, Object> pFunction = this.parsers.get(targetClass);
		if(pFunction == null) {
			throw new ClassNotInContextException(this, targetClass);
		}
		return pFunction;
	}
	
	/**
	 * Default parsing context
	 */
	public static final ValueParserContext DEFAULT = 
			new ValueParserContext(Map.of(
						String.class, (String x) -> x,
						Integer.class, (String x) -> Integer.parseInt(x),
						Double.class, (String x) -> Double.parseDouble(x),
						Float.class, (String x) -> Float.parseFloat(x),
						Boolean.class, (String x) -> Boolean.parseBoolean(x),
						LocalDateTime.class, (String x) -> {
							LocalDateTime dateTime = null;
							try {
								dateTime = LocalDateTime.parse(x, DateTimeFormatterProvider.formatter());
							}catch(Exception e) {
								throw new RuntimeException(e);
							}
							return dateTime;
						},
						rq.common.types.Str10.class, (String x) -> rq.common.types.Str10.factory(x),
						rq.common.types.Str50.class, (String x) -> rq.common.types.Str50.factory(x),
						rq.common.types.DateTime.class, (String x) -> {
							LocalDateTime dateTime = null;
							try {
								dateTime = LocalDateTime.parse(x, DateTimeFormatterProvider.formatter());
							}catch(Exception e) {
								throw new RuntimeException(e);
							}
							return new rq.common.types.DateTime(dateTime);
						}
					));
}
