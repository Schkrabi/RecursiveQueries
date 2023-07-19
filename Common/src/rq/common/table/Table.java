package rq.common.table;

import java.util.Set;
import java.util.stream.Stream;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.exceptions.TypeSchemaMismatchException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;

/**
 * Represents a table of records
 * 
 * @author Mgr. R.Skrabal
 *
 */
public class Table implements Iterable<Record>, TabularExpression {
	public final Schema schema;
	private Set<Record> records = new HashSet<Record>();

	public Table(Schema schema) {
		this.schema = schema;
	}

	/**
	 * Inserts record into the table
	 * 
	 * @param record inserted record
	 * @return true if successfully inserted, false otherwise
	 */
	public boolean insert(Record record) throws TableRecordSchemaMismatch {
		// TODO What if I am inserting record with same values and different rank?
		// Theoretically, if the rank of the new record is higher, it should overwrite
		// the existing one (since it belongs to the relation more than before) and if
		// it is lesser, it should be ignored.
		if (!this.schema.equals(record.schema)) {
			throw new TableRecordSchemaMismatch(this.schema, record.schema);
		}
		return this.records.add(record);
	}

	/**
	 * Inserts record into the database 
	 * @param values named values
	 * @param rank rank of the record
	 * @return true if inserted, false otherwise
	 * @throws AttributeNotInSchemaException 
	 * @throws TypeSchemaMismatchException 
	 * @throws TableRecordSchemaMismatch 
	 */
	public boolean insert(Collection<Record.AttributeValuePair> values, double rank)
			throws TypeSchemaMismatchException, AttributeNotInSchemaException, TableRecordSchemaMismatch {
		Record r = Record.factory(this.schema, values, rank);
		return this.insert(r);
	}

	/**
	 * Deletes record from the table
	 * 
	 * @param record
	 * @return true if successfully deleted, false otherwise
	 */
	public boolean delete(Record record) throws TableRecordSchemaMismatch {
		if (!this.schema.equals(record.schema)) {
			throw new TableRecordSchemaMismatch(this.schema, record.schema);
		}
		return this.records.remove(record);
	}

	/**
	 * Updates record in this table
	 * 
	 * @param orig   original record
	 * @param record updated reocrd
	 * @return true if successful false otherwise
	 */
	public boolean update(Record orig, Record record) throws TableRecordSchemaMismatch {
		if (!this.schema.equals(record.schema)) {
			throw new TableRecordSchemaMismatch(this.schema, record.schema);
		}
		if (!this.schema.equals(orig.schema)) {
			throw new TableRecordSchemaMismatch(this.schema, orig.schema);
		}
		if (this.delete(orig)) {
			return this.insert(record);
		}
		return false;
	}

	@Override
	public Iterator<Record> iterator() {
		return this.records.iterator();
	}

	/**
	 * Gets stream of this table records
	 * 
	 * @return Stream object
	 */
	public Stream<Record> stream() {
		return this.records.stream();
	}

	@Override
	public Table eval() {
		return this;
	}

	@Override
	public Schema schema() {
		return this.schema;
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("{")
				.append(this.records.stream().map(r -> r.toString())
						.reduce((s1, s2) -> new StringBuilder().append(s1).append(" ").append(s2).toString()))
				.append("}")
				.toString();
	}
	
	/**
	 * Returns true if this table contains record r. Returns false otherwise.
	 * @param r
	 * @return true or false
	 */
	public boolean contains(Record r) {
		return this.records.contains(r);
	}
	
	/**
	 * Returns true if this table contains given record, not taking rank into account. Returns false otherwise.
	 * @param r
	 * @return true or false
	 */
	public boolean containsNoRank(Record r) {
		return this.stream().anyMatch(x -> r.equalsNoRank(x));
	}
	
	/**
	 * Tries to find specific record without taking rank into account.
	 * @param r
	 * @return Oprional of recod if it is present, Empty optional otherwise.
	 */
	public Optional<Record> findNoRank(Record r) {
		return this.stream().filter(x -> r.equalsNoRank(x)).findAny();
	}
}
