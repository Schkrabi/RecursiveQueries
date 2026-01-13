package rq.common.onOperators;

import rq.common.table.Record;
import rq.common.table.Schema;

/**
 * Record value adding two subexpressions, only supports subexpressions from the same record
 */
public class PlusDouble implements RecordValue<Double> {
	
	private final RecordValue<Double> left;
	private final RecordValue<Double> right;
	
	public PlusDouble(RecordValue<Double> left, RecordValue<Double> right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public Double value(Record record) {
		var leftDouble = left.value(record);
		var rightDouble = right.value(record);
		
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
	public Class<Double> domain() {
		return Double.class;
	}

}
