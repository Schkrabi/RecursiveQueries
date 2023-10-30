package rq.common.onOperators;

import java.time.Duration;

import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.types.DateTime;

public class PlusDateTime implements RecordValue {
	
	private final RecordValue left;
	private final Duration right;
	
	public PlusDateTime(RecordValue left, Duration right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public DateTime value(Record record) {
		DateTime leftDateTime = (DateTime)left.value(record);
		return new DateTime(leftDateTime.getInner().plus(this.right));
	}

	@Override
	public boolean isApplicableToSchema(Schema schema) {
		Class<?> leftDomain = this.left.domain();
		
		if(!leftDomain.equals(DateTime.class)) {
			return false;
		}
		
		return this.left.isApplicableToSchema(schema);
	}

	@Override
	public Class<?> domain() {
		return DateTime.class;
	}

}
