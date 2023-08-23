/**
 * 
 */
package rq.common.onOperators;

import java.util.function.BiFunction;

import rq.common.table.Attribute;
import rq.common.table.Record;

/**
 * Represents SIMILAR ON clause
 * @author Mgr. Radomir Skrabal
 *
 */
public class OnSimilar extends OnOperator {
	
	public final BiFunction<Object, Object, Double> similarity;

	public OnSimilar(Attribute left, Attribute right, BiFunction<Object, Object, Double> similarity) {
		super(left, right);
		this.similarity = similarity;
	}

	@Override
	public double eval(Record leftRecord, Record rightRecord) {
		Object leftValue = leftRecord.getNoThrow(this.left);
		Object rightValue = rightRecord.getNoThrow(this.right);
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
