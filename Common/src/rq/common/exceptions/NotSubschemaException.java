/**
 * 
 */
package rq.common.exceptions;

import rq.common.table.Schema;

/**
 * Raised when trying to make projection on improper schema
 * @author Mgr. R.Skrabal
 *
 */
public class NotSubschemaException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5337729362852902906L;
	public final Schema superschema;
	public final Schema subschema;
	
	public NotSubschemaException(Schema superschema, Schema subschema) {
		super(new StringBuilder()
				.append("Schema ")
				.append(subschema)
				.append(" is not subschema of ")
				.append(superschema)
				.toString());
		this.superschema = superschema;
		this.subschema = subschema;
	}
}
