/**
 * 
 */
package rq.common.exceptions;

import rq.common.table.Attribute;
import rq.common.table.Schema;

/**
 * Raised when record is requested for attribute value that is not in its schema
 * @author Mgr. R.Skrabal
 *
 */
public class AttributeNotInSchemaException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 822959249214795413L;
	public final Attribute attribute;
	public final String attributeName;
	public final Schema schema;
	
	public AttributeNotInSchemaException(Attribute attribute, Schema schema) {
		super(new StringBuilder()
				.append("Attribute ")
				.append(attribute)
				.append(" is not present in schema ")
				.append(schema)
				.toString());
		this.attribute = attribute;
		this.schema = schema;
		this.attributeName = attribute.name;
	}
	
	public AttributeNotInSchemaException(String name, Schema schema) {
		super(new StringBuilder()
				.append("Attribute ")
				.append(name)
				.append(" is not present in schema ")
				.append(schema)
				.toString());
		this.attribute = null;
		this.schema = schema;
		this.attributeName = name;
	}
}
