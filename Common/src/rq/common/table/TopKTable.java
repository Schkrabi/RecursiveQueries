/**
 * 
 */
package rq.common.table;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.stream.Stream;
import java.util.List;
import java.util.LinkedList;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.interfaces.Table;
import rq.common.table.Record.AttributeValuePair;

/**
 * @author Mgr. Radomir Skrabal
 *
 */
public class TopKTable implements Table {
	
	private final int k;
	private final Schema schema;
	private final PriorityQueue<Record> records = new PriorityQueue<Record>(Record.RANK_COMPARATOR_ASC);
	
	private TopKTable(Schema schema, int k) {
		this.k = k;
		this.schema = schema;
	}
	
	public static TopKTable factory(Schema schema, int k) {
		return new TopKTable(schema, k);
	}
	
	public double minRank() {
		if(!this.isEmpty()) {
			return this.records.peek().rank;
		}
		return 0.0d;
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
	public Iterator<Record> iterator() {
		return this.records.iterator();
	}

	@Override
	public boolean insert(Record record) throws TableRecordSchemaMismatch {
		if(!record.schema.equals(this.schema)) {
			throw new TableRecordSchemaMismatch(this.schema, record.schema);
		}
		
		this.records.add(record);
		if(this.records.size() <= k) {
			return true;
		}
		
		double minRank = this.minRank();
		List<Record> excluded = new LinkedList<Record>();
		while(this.minRank() == minRank) {
			excluded.add(this.records.poll());
		}
		
		if(this.size() <= k) {
			return true;
		}
		
		this.records.addAll(excluded);
		
		return true;
	}

	@Override
	public boolean insert(Collection<AttributeValuePair> values, double rank)
			throws TypeSchemaMismatchException, AttributeNotInSchemaException, TableRecordSchemaMismatch {
		Record r = Record.factory(this.schema, values, rank);
		return this.insert(r);
	}

	@Override
	public boolean delete(Record record) throws TableRecordSchemaMismatch {
		return this.records.remove(record);
	}

	@Override
	public Stream<Record> stream() {
		return this.records.stream();
	}

	@Override
	public boolean contains(Record record) {
		return this.records.contains(record);
	}

	@Override
	public boolean containsNoRank(Record record) {
		return this.stream().anyMatch(r -> record.equalsNoRank(r));
	}

	@Override
	public Optional<Record> findNoRank(Record record) {
		return this.stream().filter(r -> record.equalsNoRank(r)).findFirst();
	}

	@Override
	public boolean isEmpty() {
		return this.records.isEmpty();
	}

	@Override
	public int size() {
		return this.records.size();
	}

}