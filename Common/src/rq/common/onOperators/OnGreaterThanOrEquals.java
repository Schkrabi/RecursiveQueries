/**
 * 
 */
package rq.common.onOperators;

import rq.common.table.Record;

/**
 * @author Mgr. Radomir Skrabal
 *
 */
public class OnGreaterThanOrEquals<T extends Comparable<T>> extends OnCompare<T> {

	public OnGreaterThanOrEquals(RecordValue<T> left, RecordValue<T> right) {
		super(left, right);
	}
	
	public static <T extends Comparable<T>> OnGreaterThanOrEquals<T> factory(RecordValue<T> left, RecordValue<T> right) {
		return new OnGreaterThanOrEquals<>(left, right);
	}

	@Override
	public double eval(Record leftRecord, Record rightRecord) {
		var leftValue = this.left.value(leftRecord);
		var rightValue = this.right.value(rightRecord);
		
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
