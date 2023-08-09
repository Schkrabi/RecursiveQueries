/**
 * 
 */
package rq.common.operators;

import java.util.function.Function;

import rq.common.table.Schema;
import rq.common.table.Table;
import rq.common.table.TabularExpression;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.table.Record;

/**
 * @author Mgr. R.Skrabal
 *
 */
public class Map implements TabularExpression {
	
	private final TabularExpression argument;
	private final Function<Record, Record> fun;
	private final Function<Schema, Schema> sFun;

	public Map(TabularExpression argument, Function<Record, Record> fun, Function<Schema, Schema> sFun) {
		this.argument = argument;
		this.fun = fun;
		this.sFun = sFun;
	}
	
	public Map(TabularExpression argument, Function<Record, Record> fun) {
		this.argument = argument;
		this.fun = fun;
		this.sFun = Function.identity();
	}
	
	@Override
	public Table eval() {
		Schema schema = this.schema();
		
		Table table = this.argument.eval();
		Table ret = new Table(schema);
		
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
}
