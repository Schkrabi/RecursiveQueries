package rq.common.onOperators;

import rq.common.table.Record;
import rq.common.table.Schema;

public class PlusInteger implements RecordValue {
	
	private final RecordValue left;
	private final RecordValue right;
	
	public PlusInteger(RecordValue left, RecordValue right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public Object value(Record record) {
		Integer iLeft = (Integer)left.value(record);
		Integer iRight = (Integer)right.value(record);
				
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
	public Class<?> domain() {
		return Integer.class;
	}

}
