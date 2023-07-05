package rq.common.table;

import java.util.Set;
import java.util.HashSet;

/**
 * Represents a table of records
 * @author Mgr. R.Skrabal
 *
 */
public class Table {
	public final Schema schema;
	private Set<Record> records = new HashSet<Record>();
	
	public Table(Schema schema) {
		this.schema = schema;
	}
	
	/**
	 * Inserts record into the table
	 * @param record inserted record
	 * @return true if successfully inserted, false otherwise
	 */
	public boolean insert(Record record) {
		if(!this.schema.equals(record.schema)) {
			//TODO Throw here?
			return false;
		}
		return this.records.add(record);
	}
	
	/**
	 * Deletes record from the table
	 * @param record
	 * @return true if successfully deleted, false otherwise
	 */
	public boolean delete(Record record) {
		if(!this.schema.equals(record.schema)) {
			return false;
		}
		return this.records.remove(record);
	}
	
	/**
	 * Updates record in this table
	 * @param orig original record
	 * @param record updated reocrd
	 * @return true if successful false otherwise
	 */
	public boolean update(Record orig, Record record) {
		if(		!this.schema.equals(orig.schema)
			&&	this.delete(orig)) {
			return this.insert(record);
		}
		return false;
	}
}
