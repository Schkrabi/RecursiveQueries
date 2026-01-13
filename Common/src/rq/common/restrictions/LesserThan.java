/**
 * 
 */
package rq.common.restrictions;

import rq.common.onOperators.RecordValue;
import rq.common.table.Record;

/**
 * Lesser than selection condition
 */
public class LesserThan<T extends Comparable<T>> extends CompareCondition<T> {

	public LesserThan(RecordValue<T> left, RecordValue<T> right) {
		super(left, right);
	}

	@Override
	public double eval(Record record) {
		var leftValue = this.left.value(record);
		var rightValue = this.right.value(record);
		return leftValue.compareTo(rightValue) <= -1 ? 1.0d : 0.0d; 
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append(this.left.toString())
				.append(" < ")
				.append(this.right.toString())
				.toString();
	}
}
