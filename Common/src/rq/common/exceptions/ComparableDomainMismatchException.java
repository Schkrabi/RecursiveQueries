/**
 * 
 */
package rq.common.exceptions;

import rq.common.onOperators.RecordValue;

/**
 * @author Mgr. Radomir Skrabal
 *
 */
public class ComparableDomainMismatchException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 507942147540763204L;
	public final RecordValue left;
	public final RecordValue right;
	
	public ComparableDomainMismatchException(RecordValue left, RecordValue right) {
		super(new StringBuilder()
				.append("Compared attributes ")
				.append(left.toString())
				.append(" and ")
				.append(right.toString())
				.append(" have different domain.")
				.toString());
		this.left = left;
		this.right = right;
	}
}
