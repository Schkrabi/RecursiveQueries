/**
 * 
 */
package rq.common.onOperators;

import rq.common.table.Record;

/**
 * @author r.skrabal
 *
 */
public class OnLesserThan<T extends Comparable<T>> extends OnCompare<T> {
	
	public OnLesserThan(RecordValue<T> left, RecordValue<T> right) {
		super(left, right);
	}
	
	public static <T extends Comparable<T>> OnLesserThan<T> factory(RecordValue<T> left, RecordValue<T> right) {
			return new OnLesserThan<>(left, right);
		}

	@Override
	public double eval(Record leftRecord, Record rightRecord) {
		var leftValue = this.left.value(leftRecord);
		var rightValue = this.right.value(rightRecord);
		return leftValue.compareTo(rightValue) <= -1 ? 1.0d : 0.0d; 
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append(this.left)
				.append(" < ")
				.append(this.right)
				.toString();
	}
}
