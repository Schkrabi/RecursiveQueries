package rq.common.exceptions;

import rq.common.table.Schema;

/**
 * Thrown when tries to union or intersect tables with different schemas
 * @author Mgr. R.Skrabal
 *
 */
public class SchemaNotEqualException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2949153527248340919L;
	public final Schema schema1, schema2;
	
	public SchemaNotEqualException(Schema schema1, Schema schema2) {
		super(new StringBuilder()
				.append("Schema ")
				.append(schema1)
				.append(" and ")
				.append(schema2)
				.append(" are not equal.")
				.toString());
		this.schema1 = schema1;
		this.schema2 = schema2;
	}
}
