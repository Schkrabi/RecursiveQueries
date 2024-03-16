package rq.common.operators;

import java.util.PriorityQueue;
import java.util.function.BinaryOperator;

import rq.common.exceptions.SchemaNotEqualException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.SchemaProvider;
import rq.common.interfaces.Table;
import rq.common.latices.LaticeFactory;
import rq.common.statistic.Statistics;
import rq.common.table.Record;
import rq.common.table.Schema;

public class LazyUnion implements LazyExpression, SchemaProvider {
	
	private final Schema schema;
	
	private final Table leftTable;
	private final Table rightTable;
	
	private final PriorityQueue<Record> left;
	private final PriorityQueue<Record> right;
	
	private final BinaryOperator<Double> supremum;
	
	public static LazyUnion factory(Table left, Table right) throws SchemaNotEqualException {
		Schema schema1 = left.schema();
		Schema schema2 = right.schema();
		if (!schema1.equals(schema2)) {
			throw new SchemaNotEqualException(schema1, schema2);
		}
		return new LazyUnion(left, right, LaticeFactory.instance().getSupremum());
	}
	
	private LazyUnion(Table left, Table right, BinaryOperator<Double> supremum)  {
		this.schema = left.schema();		
		this.leftTable = left;
		this.left = new PriorityQueue<Record>();
		this.leftTable.stream().forEach(r -> this.left.add(r));
		this.rightTable = right;
		this.right = new PriorityQueue<Record>();
		this.rightTable.stream().forEach(r -> this.right.add(r));
		this.supremum = supremum;
	}

	@Override
	public Statistics getStatistics() {
		return null;
	}

	@Override
	public boolean hasStatistics() {
		return false;
	}

	@Override
	public Schema schema() {
		return this.schema;
	}

	@Override
	public Record next() {
		if(this.left.isEmpty()) {
			return this.right.poll();
		}
		if(this.right.isEmpty()) {
			return this.left.poll();
		}
		
		var lCurrent = this.left.peek();
		var rCurrent = this.right.peek();
			
		 
		if(lCurrent.equalsNoRank(rCurrent)) {
			var r = new Record(lCurrent, this.supremum.apply(lCurrent.rank, rCurrent.rank));	
			this.left.poll();
			this.right.poll();
			return r;
		}
		
		var cmp = lCurrent.compareTo(rCurrent);
		if(cmp < 0) {
			return this.left.poll();
		}
		return this.right.poll();
	}

}
