package rq.common.restrictions;

import rq.common.onOperators.RecordValue;
import rq.common.table.Record;

/**
 * Equals condition for selection
 */
public class Equals extends BiCondition {
	
	public Equals(RecordValue left, RecordValue right) {
		super(left, right);
	}

	@Override
	public double eval(Record record) {
		Object lo = this.left.value(record);
		Object ro = this.right.value(record);
		return lo.equals(ro) ? 1.0d : 0.0d;
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append(this.left.toString())
				.append(" = ")
				.append(this.right.toString())
				.toString();
	}

}
