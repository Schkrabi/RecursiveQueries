/**
 * 
 */
package rq.common.operators;

import java.util.Optional;
import java.util.function.Function;

import rq.common.table.MemoryTable;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.TabularExpression;
import rq.common.table.Record;
import rq.common.interfaces.Table;

/**
 * Almost unrestricted algorithm
 * @author Mgr. R.Skrabal
 *
 */
public class RecursiveUnrestricted extends Recursive {

	protected RecursiveUnrestricted(TabularExpression argument, Function<Table, Table> expression) {
		super(argument, expression);
	}
	
	public static RecursiveUnrestricted factory(TabularExpression argument, Function<Table, Table> expression) {
		return new RecursiveUnrestricted(argument, expression);
	}

	@Override
	protected Table aggregate() {
		Table w = this.initial.eval();
		Table r = new MemoryTable(w.schema());
		
		while(!w.isEmpty()) {
			MemoryTable n = new MemoryTable(w.schema());
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
