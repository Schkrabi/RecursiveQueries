package rq.common.restrictions;

import rq.common.onOperators.RecordValue;
import rq.common.table.Schema;

/**
 * Abstract class for double valued selection conditions
 */
public abstract class BiCondition implements SelectionCondition {
	
	public final RecordValue left;
	public final RecordValue right;
	
	public BiCondition(RecordValue left, RecordValue right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public boolean isApplicableToSchema(Schema schema) {
		return this.left.isApplicableToSchema(schema)
				&& this.right.isApplicableToSchema(schema);
	}

}
