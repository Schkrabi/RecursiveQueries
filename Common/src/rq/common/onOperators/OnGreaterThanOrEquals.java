/**
 * 
 */
package rq.common.onOperators;

import rq.common.exceptions.ComparableDomainMismatchException;
import rq.common.exceptions.NotComparableException;
import rq.common.table.Record;

/**
 * @author Mgr. Radomir Skrabal
 *
 */
public class OnGreaterThanOrEquals extends OnCompare {

	public OnGreaterThanOrEquals(RecordValue left, RecordValue right) {
		super(left, right);
	}
	
	public static OnGreaterThanOrEquals factory(RecordValue left, RecordValue right)
			throws NotComparableException, ComparableDomainMismatchException {
		validateComparable(left, right);
		
		return new OnGreaterThanOrEquals(left, right);
	}

	@SuppressWarnings("unchecked")
	@Override
	public double eval(Record leftRecord, Record rightRecord) {
		Comparable<Object> leftValue = (Comparable<Object>)this.left.value(leftRecord);
		Comparable<Object> rightValue = (Comparable<Object>)this.right.value(rightRecord);
		
		int cmp = leftValue.compareTo(rightValue); 
		
		return cmp > -1 ? 1.0d : 0.0d; 
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append(this.left)
				.append(" >= ")
				.append(this.right)
				.toString();
	}
}
