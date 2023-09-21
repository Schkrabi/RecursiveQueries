package rq.common.interfaces;

import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.table.Record;
import rq.common.table.MemoryTable;

public interface LazyExpression extends SchemaProvider {
	public abstract Record next();
	
	/**
	 * Realizes lazy expression into the table
	 * @param exp
	 * @return
	 */
	public static MemoryTable realize(LazyExpression exp) {
		MemoryTable table = null;
		Record record = exp.next();
		while(record != null) {
			if(table == null) {
				table = new MemoryTable(record.schema);
			}
			try {
				table.insert(record);
			} catch (TableRecordSchemaMismatch e) {
				//Unlikely
				throw new RuntimeException(e);
			}
		}
		return table;
	}
}
