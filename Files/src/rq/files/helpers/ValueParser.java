package rq.files.helpers;

import rq.common.table.Attribute;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.io.ValueParserContext;

/**
 * Object for parsing an attribute value
 * 
 * @author Mgr. R.Skrabal
 *
 */
public class ValueParser {
	private final Attribute attribute;
	private final String parsed;
	private final ValueParserContext context;
	
	public ValueParser(Attribute attribute, String parsed) {
		this.attribute = attribute;
		this.parsed = parsed;		
		this.context = ValueParserContext.DEFAULT;
	}
	
	public ValueParser(Attribute attribute, String parsed, ValueParserContext context) {
		this.attribute = attribute;
		this.parsed = parsed;
		this.context = context;
	}
	
	/**
	 * Parses the value
	 * @return parsed value
	 * @throws ClassNotInContextException
	 */
	public Object parse() 
		throws ClassNotInContextException {
		Object oParsed = this.context.parseValue(attribute.domain, this.parsed);
		return oParsed;
	}
	
	/**
	 * Parses the value
	 * @param attribute
	 * @param value
	 * @return
	 * @throws ClassNotInContextException
	 */
	public static Object parse(Attribute attribute, String value) 
		throws ClassNotInContextException {
		return ValueParser.parse(attribute, value, ValueParserContext.DEFAULT);
	}
	
	public static Object parse(Attribute attribute, String value, ValueParserContext context) 
		throws ClassNotInContextException {
		ValueParser parser = new ValueParser(attribute, value, context);
		return parser.parse();
	}
}
