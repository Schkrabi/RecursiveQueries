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

public class LazyIntersection implements LazyExpression, SchemaProvider {
	
	private final Schema schema;
	private final Table leftTable;
	private final PriorityQueue<Record> left = new PriorityQueue<Record>(); 
	private final Table rightTable;
	private final PriorityQueue<Record> right = new PriorityQueue<Record>();
	private final BinaryOperator<Double> infimum;
	
	public static LazyIntersection factory(Table left, Table right) throws SchemaNotEqualException {
		Schema schema1 = left.schema();
		Schema schema2 = right.schema();
		if (!schema1.equals(schema2)) {
			throw new SchemaNotEqualException(schema1, schema2);
		}
		return new LazyIntersection(left, right, LaticeFactory.instance().getInfimum());
	}
	
	private LazyIntersection(
			Table left,
			Table right,
			BinaryOperator<Double> infimum) {
		this.schema = left.schema();
		this.leftTable = left;
		this.leftTable.stream().forEach(r -> this.left.add(r));
		this.rightTable = right;
		this.rightTable.stream().forEach(r -> this.right.add(r));
		this.infimum = infimum;
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
			return null;
		}
		if(this.right.isEmpty()) {
			return null;
		}
		
		var lCurrent = this.left.peek();
		var rCurrent = this.right.peek();
		
		while(!lCurrent.equalsNoRank(rCurrent)) {
			if(lCurrent.compareTo(rCurrent) < 0) {
				this.left.poll();
				lCurrent = this.left.peek();
			}
			else {
				this.right.poll();
				rCurrent = this.right.peek();
			}
			if(lCurrent == null || rCurrent == null) {
				return null;
			}
		}
		
		var rec = new Record(lCurrent, this.infimum.apply(lCurrent.rank, rCurrent.rank));
		this.left.poll();
		this.right.poll();
		return rec;
	}

}
