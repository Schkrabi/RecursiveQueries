/**
 * 
 */
package rq.common.operators;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import rq.common.table.MemoryTable;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.TabularExpression;
import rq.common.table.Record;
import rq.common.interfaces.Table;
import rq.common.table.Schema;

/**
 * Almost unrestricted algorithm
 * @author Mgr. R.Skrabal
 *
 */
public class RecursiveUnrestricted extends Recursive {
	
	private final BiFunction<Schema, Integer, Table> tableSupplier;

	protected RecursiveUnrestricted(TabularExpression argument, Function<Table, Table> expression, BiFunction<Schema, Integer, Table> tableSupplier) {
		super(argument, expression);
		this.tableSupplier = tableSupplier;
	}
	
	public static RecursiveUnrestricted factory(TabularExpression argument, Function<Table, Table> expression, BiFunction<Schema, Integer, Table> tableSupplier) {
		return new RecursiveUnrestricted(argument, expression, tableSupplier);
	}
	
	public static RecursiveUnrestricted factory(TabularExpression argument, Function<Table, Table> expression) {
		return RecursiveUnrestricted.factory(argument, expression, (Schema s, Integer count) -> new MemoryTable(s));
	}

	@Override
	protected Table aggregate() {
		Table w = this.initial.eval();
		Table r = this.tableSupplier.apply(w.schema(), w.size());
		
		while(!w.isEmpty()) {
			//TODO maybe add supplier even for intermediate table
			Table n = new MemoryTable(w.schema());
			for(Record rc : w) {
				Optional<Record> o = r.findNoRank(rc);
				if(		o.isEmpty()
					||	o.get().rank < rc.rank) {
					try {
						n.insert(rc);
						if(o.isPresent()) {
							r.delete(o.get());
						}
						r.insert(rc);
					} catch (TableRecordSchemaMismatch e) {
						//Unlikely
						throw new RuntimeException(e);
					}
				}
			}
			w = this.expression.apply(n);
		}
		
		return r;
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("WITH RECURSIVE ")
				.append(this.initial.toString())
				.append(" UNION ALL ")
				.append(this.expression.toString())
				.toString();
	}

}
