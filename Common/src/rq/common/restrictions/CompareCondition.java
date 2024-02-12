/**
 * 
 */
package rq.common.restrictions;

import rq.common.exceptions.ComparableDomainMismatchException;
import rq.common.exceptions.NotComparableException;
import rq.common.onOperators.OnGreaterThan;
import rq.common.onOperators.RecordValue;

/**
 * 
 */
public abstract class CompareCondition extends BiCondition {

	
	
	public CompareCondition(RecordValue left, RecordValue right) {
		super(left, right);
		validateComparable(left, right);
	}
	
	/**
	 * Validates arguments for being comparable
	 * @param left argument
	 * @param right argument
	 * @return true if argument validates. Throws otherwise
	 */
	protected static boolean validateComparable(RecordValue left, RecordValue right) {
		if (!Comparable.class.isAssignableFrom(left.domain())) {
			throw new RuntimeException(new NotComparableException(left, OnGreaterThan.class));
		}
		if (!Comparable.class.isAssignableFrom(right.domain())) {
			throw new RuntimeException(new NotComparableException(right, OnGreaterThan.class));
		}
		if (!left.domain().equals(right.domain())) {
			throw new RuntimeException(new ComparableDomainMismatchException(left, right));
		}
		
		return true;
	}

}
