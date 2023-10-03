package rq.common.operators;

import java.util.Optional;
import java.util.function.Function;

import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.table.MemoryTable;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.table.TopKTable;

public class LazyRecursiveTopK extends LazyRecursive {
	
	private final Function<Schema, Table> intermediateTableProvider;
	private final int k;

	protected LazyRecursiveTopK(
			LazyExpression argExp, 
			Function<Table, LazyExpression> funExp,
			Function<Schema, Table> intermediateTableProvider,
			int k) {
		super(argExp, funExp);
		this.intermediateTableProvider = intermediateTableProvider;
		this.k = k;
	}
	
	public static LazyRecursiveTopK factory(
			LazyExpression argExp,
			Function<Table, LazyExpression> funExp,
			int k,
			Function<Schema, Table> intermediateTableProvider) {
		return new LazyRecursiveTopK(
				argExp, funExp, intermediateTableProvider, k);
	}
	
	public static LazyRecursiveTopK factory(
			LazyExpression argExp,
			Function<Table, LazyExpression> funExp,
			int k) {
		return LazyRecursiveTopK.factory(
				argExp, 
				funExp,
				k,
				(Schema s) -> new MemoryTable(s));
	}

	@Override
	public Table eval() {
		LazyExpression w = this.argExp;
		TopKTable r = TopKTable.factory(this.schema(), this.k);
		Table n = this.intermediateTableProvider.apply(this.argExp.schema());
		
		Record record = w.next();
		while (record != null) {			
			Optional<Record> o = r.findNoRank(record);
			if (	(o.isEmpty() || o.get().rank < record.rank) 
				&&	(r.size() <= this.k || record.rank >= r.minRank())) {
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
				w = this.funExpr.apply(n);
				n = this.intermediateTableProvider.apply(this.argExp.schema());
				record = w.next();
			}
		}
		
		return r;
	}
}
