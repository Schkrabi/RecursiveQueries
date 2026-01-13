/**
 * 
 */
package rq.common.restrictions;

import rq.common.onOperators.RecordValue;

/**
 * 
 */
public abstract class CompareCondition<T extends Comparable<T>> extends BiCondition<T> {

	
	
	public CompareCondition(RecordValue<T> left, RecordValue<T> right) {
		super(left, right);
		validateComparable(left, right);
	}
	
	/**
	 * Validates arguments for being comparable
	 * @param left argument
	 * @param right argument
	 * @return true if argument validates. Throws otherwise
	 */
	protected static <T extends Comparable<T>> boolean validateComparable(RecordValue<T> left, RecordValue<T> right) {
//		if (!Comparable.class.isAssignableFrom(left.domain())) {
//			throw new RuntimeException(new NotComparableException(left, OnGreaterThan.class));
//		}
//		if (!Comparable.class.isAssignableFrom(right.domain())) {
//			throw new RuntimeException(new NotComparableException(right, OnGreaterThan.class));
//		}
//		if (!left.domain().equals(right.domain())) {
//			throw new RuntimeException(new ComparableDomainMismatchException(left, right));
//		}
		
		return true;
	}

}
