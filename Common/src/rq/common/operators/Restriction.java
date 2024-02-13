package rq.common.operators;

import java.util.function.BiFunction;
import java.util.function.Function;

import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.TabularExpression;
import rq.common.statistic.Statistics;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.table.MemoryTable;
import rq.common.interfaces.Table;

/**
 * Represents crisp restriction operation on table
 * @author Mgr. R.Skrabal
 *
 */
public class Restriction implements TabularExpression {
	private final TabularExpression argument;
	private final Function<Record, Double> predicate;
	private final BiFunction<Schema, Integer, Table> tableSupplier;
	
	public Restriction(TabularExpression argument, Function<Record, Double> predicate) {
		this.argument = argument;
		this.predicate = predicate;
		this.tableSupplier = (Schema s, Integer count) -> new MemoryTable(s);
	}
	
	public Restriction(TabularExpression argument, Function<Record, Double> predicate, BiFunction<Schema, Integer, Table> tableSupplier) {
		this.argument = argument;
		this.predicate = predicate;
		this.tableSupplier = tableSupplier;
	}
	
	@Override
	public Table eval() {
		Table table = this.argument.eval();
		Table ret = this.tableSupplier.apply(table.schema(), table.size());		
		
		for(Record r : table) {
			Double rank = this.predicate.apply(r);
			if(rank > 0.0d) {
				try {
					ret.insert(new Record(r, rank));
				} catch (TableRecordSchemaMismatch e) {
					// Unlikely
					throw new RuntimeException(e);
				}
			}
		}
		
		return ret;
	}

	@Override
	public Schema schema() {
		return this.argument.schema();
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("SELECT ")
				.append(this.argument.toString())
				.append(" WHERE ")
				.append(this.predicate.toString())
				.toString();
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
