package rq.files.helpers;

import rq.common.io.contexts.ClassNotInContextException;
import rq.common.io.contexts.ValueParserContext;
import rq.common.table.Attribute;

/**
 * Object for parsing an attribute value
 * 
 * @author Mgr. R.Skrabal
 *
 */
public class ValueParser<T> {
	private final Attribute<T> attribute;
	private final String parsed;
	private final ValueParserContext context;
	
	public ValueParser(Attribute<T> attribute, String parsed) {
		this.attribute = attribute;
		this.parsed = parsed;		
		this.context = ValueParserContext.DEFAULT;
	}
	
	public ValueParser(Attribute<T> attribute, String parsed, ValueParserContext context) {
		this.attribute = attribute;
		this.parsed = parsed;
		this.context = context;
	}
	
	/**
	 * Parses the value
	 * @return parsed value
	 * @throws ClassNotInContextException
	 */
	public T parse() 
		throws ClassNotInContextException {
		if(attribute.domain == Double.class && this.parsed == "") {
			System.out.println();
		}
		var oParsed = this.context.parseValue(attribute.domain, this.parsed);
		return oParsed;
	}
	
	/**
	 * Parses the value
	 * @param attribute
	 * @param value
	 * @return
	 * @throws ClassNotInContextException
	 */
	public static <T> T parse(Attribute<T> attribute, String value) 
		throws ClassNotInContextException {
		return ValueParser.parse(attribute, value, ValueParserContext.DEFAULT);
	}
	
	public static <T> T parse(Attribute<T> attribute, String value, ValueParserContext context) 
		throws ClassNotInContextException {
		var parser = new ValueParser<T>(attribute, value, context);
		return parser.parse();
	}
}
