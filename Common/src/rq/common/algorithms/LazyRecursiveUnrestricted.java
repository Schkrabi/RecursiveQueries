/**
 * 
 */
package rq.common.algorithms;

import java.util.Optional;
import java.util.function.Function;

import rq.common.annotations.Algorithm;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.statistic.Statistics;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.tools.AlgorithmMonitor;
import rq.common.table.MemoryTable;

/**
 * @author Mgr, Radomir Skrabal
 *
 */
@Algorithm("unrestricted")
public class LazyRecursiveUnrestricted extends LazyRecursive {
	
	private final Function<Schema, Table> returnTableProvider;
	private final Function<Schema, Table> intermediateTableProvider;

	protected LazyRecursiveUnrestricted(
			LazyExpression arg, 
			Function<Table, LazyExpression> fun,
			Function<Schema, Table> returnTableProvider,
			Function<Schema, Table> intermediateTableProvider,
			AlgorithmMonitor monitor) {
		super(arg, fun, monitor);
		this.returnTableProvider = returnTableProvider;
		this.intermediateTableProvider = intermediateTableProvider;
	}
	
	public static LazyRecursiveUnrestricted factory(
			LazyExpression arg, 
			Function<Table, LazyExpression> fun,
			AlgorithmMonitor monitor) {
		return LazyRecursiveUnrestricted.factory(
				arg, 
				fun,
				(Schema s) -> new MemoryTable(s),
				(Schema s) -> new MemoryTable(s),
				monitor);
	}
	
	public static LazyRecursiveUnrestricted factory(
			LazyExpression arg, 
			Function<Table, LazyExpression> fun,
			Function<Schema, Table> returnTableProvider,
			Function<Schema, Table> intermediateTableProvider,
			AlgorithmMonitor monitor) {
		return new LazyRecursiveUnrestricted(
				arg,
				fun,
				returnTableProvider,
				intermediateTableProvider,
				monitor);
	}

	@Override
	public Table eval() {
		LazyExpression w = this.argExp;
		Table r = this.returnTableProvider.apply(this.schema());
		Table n = this.intermediateTableProvider.apply(this.argExp.schema());

		Record record = w.next();
		while (record != null) {			
			this.monitor.generatedTuples.increment();
			Optional<Record> o = r.findNoRank(record);
			if (o.isEmpty() || o.get().rank < record.rank) {
				try {
					n.insert(record);
					if(o.isPresent()) {
						r.delete(o.get());
					}
					
					this.monitor.resultCandidates.increment();
					r.insert(record);
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

	@Override
	public Statistics getStatistics() {
		return null;
	}

	@Override
	public boolean hasStatistics() {
		return false;
	}
}
