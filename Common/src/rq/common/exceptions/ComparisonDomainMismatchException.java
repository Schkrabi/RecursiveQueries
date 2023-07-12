/**
 * 
 */
package rq.common.exceptions;

import rq.common.table.Attribute;

/**
 * Thrown when two attributes with different domains should be comapred
 * @author Mgr. R.Skrabal
 *
 */
public class ComparisonDomainMismatchException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6100649929719460196L;
	public final Attribute attribute1, attribute2;
	
	public ComparisonDomainMismatchException(Attribute attribute1, Attribute attribute2) {
		super(new StringBuilder()
				.append("Cannot compare attribute ")
				.append(attribute1)
				.append(" with attribute ")
				.append(attribute2)
				.append(" since they have different domains")
				.toString());
		this.attribute1 = attribute1;
		this.attribute2 = attribute2;
	}
}
