/**
 * 
 */
package rq.common.onOperators;

import rq.common.exceptions.ComparableDomainMismatchException;
import rq.common.exceptions.NotComparableException;
import rq.common.table.Record;

/**
 * Represents grater than operator
 * @author Mgr. Radomir Skrabal
 *
 */
public class OnGreaterThan extends OnCompare {

	public OnGreaterThan(RecordValue left, RecordValue right) {
		super(left, right);
	}
	
	public static OnGreaterThan factory(RecordValue left, RecordValue right) 
		throws NotComparableException, ComparableDomainMismatchException {
		validateComparable(left, right);
		
		return new OnGreaterThan(left, right);
	}

	@SuppressWarnings("unchecked")
	@Override
	public double eval(Record leftRecord, Record rightRecord) {
		Comparable<Object> leftValue = (Comparable<Object>)this.left.value(leftRecord);
		Comparable<Object> rightValue = (Comparable<Object>)this.right.value(rightRecord);
		return leftValue.compareTo(rightValue) >= 1 ? 1.0d : 0.0d; 
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append(this.left)
				.append(" > ")
				.append(this.right)
				.toString();
	}
}
