package rq.common.onOperators;

import rq.common.table.Record;
import rq.common.table.Schema;

/**
 * Record value adding two subexpressions, only supports subexpressions from the same record
 */
public class PlusDouble implements RecordValue {
	
	private final RecordValue left;
	private final RecordValue right;
	
	public PlusDouble(RecordValue left, RecordValue right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public Double value(Record record) {
		Double leftDouble = (Double)left.value(record);
		Double rightDouble = (Double)right.value(record);
		
		return leftDouble + rightDouble;
	}

	@Override
	public boolean isApplicableToSchema(Schema schema) {
		Class<?> leftDomain = this.left.domain();
		Class<?> rightDomain = this.right.domain();
		
		if(!leftDomain.equals(Double.class)
				|| !rightDomain.equals(Double.class)) {
			return false;
		}
		
		return this.left.isApplicableToSchema(schema)
				&& this.right.isApplicableToSchema(schema);
	}

	@Override
	public Class<?> domain() {
		return Double.class;
	}

}
