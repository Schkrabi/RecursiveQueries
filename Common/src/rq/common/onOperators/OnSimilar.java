/**
 * 
 */
package rq.common.onOperators;

import java.util.function.BiFunction;

import rq.common.table.Record;

/**
 * Represents SIMILAR ON clause
 * @author Mgr. Radomir Skrabal
 *
 */
public class OnSimilar extends OnOperator {
	
	public final BiFunction<Object, Object, Double> similarity;

	public OnSimilar(RecordValue left, RecordValue right, BiFunction<Object, Object, Double> similarity) {
		super(left, right);
		this.similarity = similarity;
	}

	@Override
	public double eval(Record leftRecord, Record rightRecord) {
		Object leftValue = this.left.value(leftRecord);
		Object rightValue = this.right.value(rightRecord);
		return similarity.apply(leftValue, rightValue);
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append(this.left)
				.append(" ~ ")
				.append(this.right)
				.toString();
	}
}
