/**
 * 
 */
package rq.common.operators;

import java.util.Optional;
import java.util.function.Function;

import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.LazyExpression;
import rq.common.table.Record;
import rq.common.table.MemoryTable;

/**
 * @author Mgr, Radomir Skrabal
 *
 */
public class LazyRecursiveUnrestricted extends LazyRecursive {

	protected LazyRecursiveUnrestricted(LazyExpression arg, Function<LazyExpression, LazyExpression> fun) {
		super(arg, fun);
	}

	public static LazyRecursiveUnrestricted factory(LazyExpression arg, Function<LazyExpression, LazyExpression> fun) {
		return new LazyRecursiveUnrestricted(arg, fun);
	}

	@Override
	public MemoryTable eval() {
		LazyExpression w = this.argExp;
		MemoryTable r = new MemoryTable(this.schema());
		MemoryTable n = new MemoryTable(this.argExp.schema());

		Record record = w.next();
		while (record != null) {
			Optional<Record> o = r.findNoRank(record);
			if (o.isEmpty() || o.get().rank < record.rank) {
				try {
					n.insert(record);
					if (o.isPresent()) {
						r.delete(o.get());
					}
					r.insert(record);
				} catch (TableRecordSchemaMismatch e) {
					// Unlikely
					throw new RuntimeException(e);
				}
			}

			record = w.next();
			if (record == null) {
				LazyExpression le = n.getLazyFacade();
				w = this.funExpr.apply(le);
				n = new MemoryTable(this.argExp.schema());
				record = w.next();
			}
		}

		return r;
	}
}
