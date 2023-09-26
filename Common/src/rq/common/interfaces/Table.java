package rq.common.interfaces;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.table.Record;

public interface Table extends TabularExpression, Iterable<Record>{

	/**
	 * Inserts recoed into the table
	 * @param record
	 * @return
	 * @throws TableRecordSchemaMismatch 
	 */
	boolean insert(Record record) throws TableRecordSchemaMismatch;

	/**
	 * Inserts record into the database 
	 * @param values named values
	 * @param rank rank of the record
	 * @return true if inserted, false otherwise
	 * @throws AttributeNotInSchemaException 
	 * @throws TypeSchemaMismatchException 
	 * @throws TableRecordSchemaMismatch 
	 */
	boolean insert(Collection<Record.AttributeValuePair> values, double rank)
			throws TypeSchemaMismatchException, AttributeNotInSchemaException, TableRecordSchemaMismatch;

	boolean delete(Record record) throws TableRecordSchemaMismatch;

	/**
	 * Gets the stream of records in this table
	 * @return
	 */
	Stream<Record> stream();

	boolean contains(Record record);

	boolean containsNoRank(Record record);
	
	Optional<rq.common.table.Record> findNoRank(rq.common.table.Record record);

	boolean isEmpty();
	
	/**
	 * Returns number of records in the table
	 * @return
	 */
	int size();

}