/**
 * 
 */
package rq.common.onOperators;

import rq.common.exceptions.ComparableDomainMismatchException;
import rq.common.exceptions.NotComparableException;
import rq.common.table.Attribute;
import rq.common.table.Record;

/**
 * @author Mgr. Radomir Skrabal
 *
 */
public class OnLesserThanOrEquals extends OnCompare {

	private OnLesserThanOrEquals(Attribute left, Attribute right) {
		super(left, right);
	}
	
	public static OnLesserThanOrEquals factory(Attribute left, Attribute right)
			throws NotComparableException, ComparableDomainMismatchException {
		validateComparable(left, right);

		return new OnLesserThanOrEquals(left, right);
	}

	@SuppressWarnings("unchecked")
	@Override
	public double eval(Record leftRecord, Record rightRecord) {
		Comparable<Object> leftValue = (Comparable<Object>)leftRecord.getNoThrow(this.left);
		Comparable<Object> rightValue = (Comparable<Object>)rightRecord.getNoThrow(this.right);
		
		int cmp = leftValue.compareTo(rightValue); 
		
		return cmp != 1 ? 1.0d : 0.0d; 
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append(this.left)
				.append(" <= ")
				.append(this.right)
				.toString();
	}
}
