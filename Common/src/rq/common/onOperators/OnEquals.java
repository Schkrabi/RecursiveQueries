/**
 * 
 */
package rq.common.onOperators;

import rq.common.table.Record;

/**
 * Represents the equals experssion in ON clause 
 * @author Mgr. Radomir Skrabal
 *
 */
public class OnEquals<T> extends OnOperator<T> {

	public OnEquals(RecordValue<T> left, RecordValue<T> right) {
		super(left, right);
	}

	@Override
	public double eval(Record leftRecord, Record rightRecord) {
		var leftValue = this.left.value(leftRecord);
		var rightValue = this.right.value(rightRecord);
		return leftValue.equals(rightValue) ? 1.0d : 0.0d;
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append(this.left)
				.append(" = ")
				.append(this.right)
				.toString();
	}

}
