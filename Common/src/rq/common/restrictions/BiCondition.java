package rq.common.restrictions;

import rq.common.onOperators.RecordValue;
import rq.common.table.Schema;

/**
 * Abstract class for double valued selection conditions
 */
public abstract class BiCondition<T> implements SelectionCondition {
	
	public final RecordValue<T> left;
	public final RecordValue<T> right;
	
	public BiCondition(RecordValue<T> left, RecordValue<T> right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public boolean isApplicableToSchema(Schema schema) {
		return this.left.isApplicableToSchema(schema)
				&& this.right.isApplicableToSchema(schema);
	}

}
