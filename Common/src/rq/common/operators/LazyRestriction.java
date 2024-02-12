/**
 * 
 */
package rq.common.operators;

import java.util.function.Function;

import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.SchemaProvider;
import rq.common.statistic.Statistics;
import rq.common.table.Record;
import rq.common.table.Schema;

/**
 * Represents lazily evaluated restriction
 * @author Mgr. Radomir Skrabal
 *
 */
public class LazyRestriction implements LazyExpression, SchemaProvider {
	
	private final LazyExpression argExp;
	private final SchemaProvider argSch;
	private final Function<Record, Double> predicate;
	
	private LazyRestriction(LazyExpression argExp, SchemaProvider argSch, Function<Record, Double> predicate) {
		this.argSch = argSch;
		this.argExp = argExp;
		this.predicate = predicate;
	}
	
	public static <T extends LazyExpression & SchemaProvider> LazyRestriction factory(T arg, Function<Record, Double> predicate) {
		return new LazyRestriction(arg, arg, predicate);
	}

	@Override
	public Schema schema() {
		return this.argSch.schema();
	}

	@Override
	public Record next() {
		Record record = this.argExp.next();
		
		while(record != null) {
			Double rank = this.predicate.apply(record);
			if(rank > 0.0d) {
				return new Record(record, rank);
			}
			record = this.argExp.next();
		}
		return null;
	}

	@Override
	public Statistics getStatistics() {
		return null;
	}

	@Override
	public boolean hasStatistics() {
		return false;
	}

}
