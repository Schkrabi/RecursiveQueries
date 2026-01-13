package rq.common.onOperators;

import rq.common.table.Record;
import rq.common.table.Schema;

public class PlusInteger implements RecordValue<Integer> {
	
	private final RecordValue<Integer> left;
	private final RecordValue<Integer> right;
	
	public PlusInteger(RecordValue<Integer> left, RecordValue<Integer> right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public Integer value(Record record) {
		var iLeft = left.value(record);
		var iRight = right.value(record);
				
		return iLeft + iRight;
	}

	@Override
	public boolean isApplicableToSchema(Schema schema) {
		Class<?> leftDomain = this.left.domain();
		Class<?> rightDomain = this.right.domain();
		
		if(!leftDomain.equals(Integer.class)
				|| !rightDomain.equals(Integer.class)) {
			return false;
		}
		
		return this.left.isApplicableToSchema(schema)
				&& this.right.isApplicableToSchema(schema);
	}

	@Override
	public Class<Integer> domain() {
		return Integer.class;
	}

}
