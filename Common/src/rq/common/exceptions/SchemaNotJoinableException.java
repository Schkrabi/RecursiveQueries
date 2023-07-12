/**
 * 
 */
package rq.common.exceptions;

import rq.common.table.Schema;

/**
 * Thrown when traing to join schme with attributes with same name but different domains
 * @author Mgr. R.Skrabal
 *
 */
public class SchemaNotJoinableException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8625162383331031907L;
	public final Schema schema1;
	public final Schema schema2;
	
	public SchemaNotJoinableException(Schema schema1, Schema schema2) {
		super(new StringBuilder()
				.append("Schema ")
				.append(schema1)
				.append(" and ")
				.append(schema2)
				.append(" are not joinable.")
				.toString());
		this.schema1 = schema1;
		this.schema2 = schema2;
	}
}
