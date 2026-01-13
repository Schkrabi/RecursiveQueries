/**
 * 
 */
package rq.common.onOperators;

import rq.common.table.Attribute;
import rq.common.table.Record;

/**
 * Represents not equals ON clause
 * @author Mgr. Radomir Skrabal
 *
 */
public class OnNotEquals<T> extends OnOperator<T> {

	public OnNotEquals(Attribute<T> left, Attribute<T> right) {
		super(left, right);
	}

	@Override
	public double eval(Record leftRecord, Record rightRecord) {
		var leftValue = this.left.value(leftRecord);
		var rightValue = this.right.value(rightRecord);
		return leftValue.equals(rightValue) ? 0.0d : 1.0d;
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append(this.left)
				.append(" <> ")
				.append(this.right)
				.toString();
	}
}
