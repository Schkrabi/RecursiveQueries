/**
 * 
 */
package rq.common.restrictions;

import rq.common.onOperators.RecordValue;
import rq.common.table.Record;

/**
 * Greater than selection condition
 */
public class GreaterThan extends CompareCondition {

	public GreaterThan(RecordValue left, RecordValue right) {
		super(left, right);
	}

	@SuppressWarnings("unchecked")
	@Override
	public double eval(Record record) {
		Comparable<Object> leftValue = (Comparable<Object>)this.left.value(record);
		Comparable<Object> rightValue = (Comparable<Object>)this.right.value(record);
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
