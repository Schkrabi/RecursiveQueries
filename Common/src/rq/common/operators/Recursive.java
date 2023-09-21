/**
 * 
 */
package rq.common.operators;

import java.util.function.Function;

import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.table.Schema;
import rq.common.table.MemoryTable;

/**
 * Represents a recursive query fixpoint operator
 * @author Mgr. R.Skrabal
 *
 */
public abstract class Recursive implements TabularExpression {

	protected final TabularExpression initial;
	protected final Function<Table, Table> expression;
	
	protected Recursive(TabularExpression argument, Function<Table, Table> expression) {
		this.initial = argument;
		this.expression = expression;
	}
	
	@Override
	public Table eval() {
		Table table = this.aggregate();
		return table;
	}
	
	/**
	 * The main loop of the recursive algorithm
	 * @return table
	 */
	protected abstract Table aggregate();

	@Override
	public Schema schema() {
		Schema s = this.applyExpressionOnEmptyTable().schema();
		return s;
	}
	
	/**
	 * Applies the expression on empty table with schema of argument.
	 * @return
	 */
	protected Table applyExpressionOnEmptyTable() {
		Table table = new MemoryTable(this.initial.schema());
		Table ret = expression.apply(table);
		return ret;
	}
}
