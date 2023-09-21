/**
 * 
 */
package rq.files.exceptions;

import rq.common.table.Schema;

/**
 * @author Mgr. Radomir Skrabal
 *
 */
public class DuplicateHeaderWriteException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 932106677667626458L;
	public final Schema original, redefined;
	
	public DuplicateHeaderWriteException(Schema original, Schema redefined) {
		super(new StringBuilder()
				.append("Schema ")
				.append(original)
				.append(" redefined to ")
				.append(redefined)
				.toString());
		this.original = original;
		this.redefined = redefined;
	}
}
