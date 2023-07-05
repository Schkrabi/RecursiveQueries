/**
 * 
 */
package rq.common.exceptions;

/**
 * Raised when a duplicate attribute name detected when creating a Schema
 * @author Mgr. R.Skrabal
 *
 */
public class DuplicateAttributeNameException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6766563227157148851L;
	public final String name;
	
	public DuplicateAttributeNameException(String name) {
		super(new StringBuilder()
				.append("Duplicate attribute name ")
				.append(name)
				.toString());
		this.name = name;
	}
}
