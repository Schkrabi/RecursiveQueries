/**
 * 
 */
package rq.common.operators;

import java.util.function.BiFunction;
import java.util.function.Function;

import rq.common.table.Schema;
import rq.common.table.MemoryTable;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.TabularExpression;
import rq.common.statistic.Statistics;
import rq.common.table.Record;
import rq.common.interfaces.Table;

/**
 * @author Mgr. R.Skrabal
 *
 */
public class Map implements TabularExpression {
	
	private final TabularExpression argument;
	private final Function<Record, Record> fun;
	private final Function<Schema, Schema> sFun;
	private final BiFunction<Schema, Integer, Table> tableSupplier;
	
	public Map(TabularExpression argument, Function<Record, Record> fun, Function<Schema, Schema> sFun, BiFunction<Schema, Integer, Table> tableSupplier) {
		this.argument = argument;
		this.fun = fun;
		this.sFun = sFun;
		this.tableSupplier = tableSupplier;
	}

	public Map(TabularExpression argument, Function<Record, Record> fun, Function<Schema, Schema> sFun) {
		this.argument = argument;
		this.fun = fun;
		this.sFun = sFun;
		this.tableSupplier = (Schema s, Integer count) -> new MemoryTable(s);
	}
	
	public Map(TabularExpression argument, Function<Record, Record> fun, BiFunction<Schema, Integer, Table> tableSupplier) {
		this.argument = argument;
		this.fun = fun;
		this.sFun = Function.identity();
		this.tableSupplier = tableSupplier;
	}
	
	public Map(TabularExpression argument, Function<Record, Record> fun) {
		this.argument = argument;
		this.fun = fun;
		this.sFun = Function.identity();
		this.tableSupplier = (Schema s, Integer count) -> new MemoryTable(s);
	}
	
	@Override
	public Table eval() {
		Schema schema = this.schema();
		
		Table table = this.argument.eval();
		Table ret = this.tableSupplier.apply(schema, table.size());
		
		table.stream().map(this.fun).forEach(r -> {
			try {
				ret.insert(r);
			} catch (TableRecordSchemaMismatch e) {
				throw new RuntimeException(e);
			}
		});
		
		return ret;
	}

	@Override
	public Schema schema() {
		return sFun.apply(this.argument.schema());
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append("MAP( ")
				.append(this.argument.toString())
				.append(")")
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
