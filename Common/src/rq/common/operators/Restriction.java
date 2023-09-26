package rq.common.operators;

import java.util.function.BiFunction;
import java.util.function.Predicate;

import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.TabularExpression;
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
	private final Predicate<Record> predicate;
	private final BiFunction<Schema, Integer, Table> tableSupplier;
	
	public Restriction(TabularExpression argument, Predicate<Record> predicate) {
		this.argument = argument;
		this.predicate = predicate;
		this.tableSupplier = (Schema s, Integer count) -> new MemoryTable(s);
	}
	
	public Restriction(TabularExpression argument, Predicate<Record> predicate, BiFunction<Schema, Integer, Table> tableSupplier) {
		this.argument = argument;
		this.predicate = predicate;
		this.tableSupplier = tableSupplier;
	}
	
	@Override
	public Table eval() {
		Table table = this.argument.eval();
		Table ret = this.tableSupplier.apply(table.schema(), table.size());
		table.stream().filter(this.predicate).forEach(r -> {
			try {
				ret.insert(r);
			} catch (TableRecordSchemaMismatch e) {
				// Unlikely
				throw new RuntimeException(e);
			}
		});
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

}
