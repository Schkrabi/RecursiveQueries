package rq.common.interfaces;

import java.io.IOException;

import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.table.Record;
import rq.common.table.FileMappedTable;
import rq.common.table.MemoryTable;

public interface LazyExpression extends SchemaProvider {
	public abstract Record next();
	
	/**
	 * Realizes lazy expression into the table
	 * @param exp
	 * @return
	 */
	public static MemoryTable realizeInMemory(LazyExpression exp) {
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
			record = exp.next();
		}
		return table;
	}
	
	/**
	 * Realizes lazy expression into file mapped table
	 * @param exp expression
	 * @param expectedCount expected count of records
	 * @return file mapped table
	 * @throws IOException
	 */
	public static FileMappedTable realizeMapped(LazyExpression exp, int expectedCount)
			throws IOException {
		FileMappedTable table = null;
		Record record = exp.next();
		while(record != null) {
			if(table == null) {
				table = FileMappedTable.factory(record.schema, expectedCount);
			}
			table.insert(record);
			record = exp.next();
		}
		return table;
	}
}
