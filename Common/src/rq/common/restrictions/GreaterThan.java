/**
 * 
 */
package rq.common.restrictions;

import rq.common.onOperators.RecordValue;
import rq.common.table.Record;

/**
 * Greater than selection condition
 */
public class GreaterThan<T extends Comparable<T>> extends CompareCondition<T> {

	public GreaterThan(RecordValue<T> left, RecordValue<T> right) {
		super(left, right);
	}

	@Override
	public double eval(Record record) {
		var leftValue = this.left.value(record);
		var rightValue = this.right.value(record);
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
