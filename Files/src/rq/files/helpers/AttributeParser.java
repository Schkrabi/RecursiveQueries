package rq.files.helpers;

import rq.common.table.Attribute;

/**
 * Parses single table header
 * @author Mgr. R.Skrabal
 *
 */
public class AttributeParser {
	private final String parsed;
	
	public AttributeParser(String parsed) {
		this.parsed = parsed;
	}
	
	/**
	 * Parses the string into an attribute
	 * @return parsed attribute
	 * @throws ClassNotFoundException
	 */
	public Attribute parse() 
			throws ClassNotFoundException {
		return Attribute.parse(parsed);
	}
	
	/**
	 * Parses the string into an attribute
	 * @param headerColumn parsed string 
	 * @return parsed attribute
	 * @throws ClassNotFoundException
	 */
	public static Attribute parse(String headerColumn)
		throws ClassNotFoundException {
		AttributeParser p = new AttributeParser(headerColumn);
		return p.parse();
	}
}
