package rq.common.onOperators;

import rq.common.table.Schema;

public interface RecordValue<T> {
	public T value(rq.common.table.Record record);
	
	public boolean isApplicableToSchema(Schema schema);
	
	public Class<? extends T> domain();
}
