/**
 * 
 */
package rq.common.operators;

import java.util.Optional;
import java.util.function.Function;

import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.tools.Counter;
import rq.common.table.MemoryTable;

/**
 * @author Mgr, Radomir Skrabal
 *
 */
public class LazyRecursiveUnrestricted extends LazyRecursive {
	
	private final Function<Schema, Table> returnTableProvider;
	private final Function<Schema, Table> intermediateTableProvider;

	protected LazyRecursiveUnrestricted(
			LazyExpression arg, 
			Function<Table, LazyExpression> fun,
			Function<Schema, Table> returnTableProvider,
			Function<Schema, Table> intermediateTableProvider,
			Counter recordCounter) {
		super(arg, fun, recordCounter);
		this.returnTableProvider = returnTableProvider;
		this.intermediateTableProvider = intermediateTableProvider;
	}

	public static LazyRecursiveUnrestricted factory(LazyExpression arg, Function<Table, LazyExpression> fun) {
		return LazyRecursiveUnrestricted.factory(arg, fun, null);
	}
	
	public static LazyRecursiveUnrestricted factory(
			LazyExpression arg, 
			Function<Table, LazyExpression> fun,
			Counter recordCounter) {
		return LazyRecursiveUnrestricted.factory(
				arg, 
				fun,
				(Schema s) -> new MemoryTable(s),
				(Schema s) -> new MemoryTable(s),
				recordCounter);
	}
	
	public static LazyRecursiveUnrestricted factory(
			LazyExpression arg, 
			Function<Table, LazyExpression> fun,
			Function<Schema, Table> returnTableProvider,
			Function<Schema, Table> intermediateTableProvider,
			Counter recordCounter) {
		return new LazyRecursiveUnrestricted(
				arg,
				fun,
				returnTableProvider,
				intermediateTableProvider,
				recordCounter);
	}
	
	public static LazyRecursiveUnrestricted factory(
			LazyExpression arg, 
			Function<Table, LazyExpression> fun,
			Function<Schema, Table> returnTableProvider,
			Function<Schema, Table> intermediateTableProvider) {
		return LazyRecursiveUnrestricted.factory(
				arg, 
				fun,
				returnTableProvider,
				intermediateTableProvider,
				null);
	}

	@Override
	public Table eval() {
		LazyExpression w = this.argExp;
		Table r = this.returnTableProvider.apply(this.schema());
		Table n = this.intermediateTableProvider.apply(this.argExp.schema());

		Record record = w.next();
		while (record != null) {			
			Optional<Record> o = r.findNoRank(record);
			if (o.isEmpty() || o.get().rank < record.rank) {
				try {
					n.insert(record);
					this.incrementCounter();
					if (o.isPresent()) {
						r.delete(o.get());
					}
					r.insert(record);
					this.incrementCounter();
				} catch (TableRecordSchemaMismatch e) {
					// Unlikely
					throw new RuntimeException(e);
				}
			}

			record = w.next();
			if (record == null) {
				w = this.funExpr.apply(n);
				n = this.intermediateTableProvider.apply(this.argExp.schema());
				record = w.next();
			}
		}

		return r;
	}
}
