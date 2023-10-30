package rq.common.onOperators;

import rq.common.table.Schema;

public interface RecordValue {
	public Object value(rq.common.table.Record record);
	
	public boolean isApplicableToSchema(Schema schema);
	
	public Class<?> domain();
}
