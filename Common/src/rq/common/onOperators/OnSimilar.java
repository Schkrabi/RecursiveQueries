/**
 * 
 */
package rq.common.onOperators;

import rq.common.table.Record;

/**
 * Represents SIMILAR ON clause
 * @author Mgr. Radomir Skrabal
 *
 */
public class OnSimilar<T> extends OnOperator<T> {
	
	public final rq.common.similarities.ISimilarity<T> similarity;

	public OnSimilar(RecordValue<T> left, RecordValue<T> right, rq.common.similarities.ISimilarity<T> similarity) {
		super(left, right);
		this.similarity = similarity;
	}

	@Override
	public double eval(Record leftRecord, Record rightRecord) {
		T leftValue = this.left.value(leftRecord);
		T rightValue = this.right.value(rightRecord);
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
