/**
 * 
 */
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
import rq.common.tools.Counter;

/**
 * @author r.skrabal
 *
 */
public class LazyRecursiveTransformed extends LazyRecursive {
	
	private final int k;
	private final Function<Schema, Table> intermediateTableProvider;
	private final Function<Record, LazyExpression> transformation;

	protected LazyRecursiveTransformed(
			LazyExpression argExp, 
			Function<Table, LazyExpression> funExp,
			int k,
			Function<Schema, Table> intermediateTableProvider,
			Function<Record, LazyExpression> transformation,
			Counter recordCounter) {
		super(argExp, funExp, recordCounter);
		this.k = k;
		this.intermediateTableProvider = intermediateTableProvider;
		this.transformation = transformation;
	}
	
	public static LazyRecursiveTransformed factory(
			LazyExpression argExp, 
			Function<Table, LazyExpression> funExp,
			int k,
			Function<Schema, Table> intermediateTableProvider,
			Function<Record, LazyExpression> transformation,
			Counter recordCounter) {
		return new LazyRecursiveTransformed(argExp, funExp, k, intermediateTableProvider, transformation, recordCounter);
	}
	
	public static LazyRecursiveTransformed factory(
			LazyExpression argExp, 
			Function<Table, LazyExpression> funExp,
			int k,
			Function<Schema, Table> intermediateTableProvider,
			Function<Record, LazyExpression> transformation) {
		return LazyRecursiveTransformed.factory(
				argExp, 
				funExp, 
				k, 
				intermediateTableProvider,
				transformation,
				null);
	}
	
	public static LazyRecursiveTransformed factory(
			LazyExpression argExp, 
			Function<Table, LazyExpression> funExp,
			int k,
			Function<Record, LazyExpression> transformation,
			Counter recordCounter) {
		return LazyRecursiveTransformed.factory(
				argExp, 
				funExp, 
				k, 
				(Schema s) -> new MemoryTable(s), 
				transformation,
				recordCounter);
	}
	
	public static LazyRecursiveTransformed factory(
			LazyExpression argExp, 
			Function<Table, LazyExpression> funExp,
			int k,
			Function<Record, LazyExpression> transformation) {
		return LazyRecursiveTransformed.factory(
				argExp,
				funExp,
				k,
				transformation,
				null);
	}

	@Override
	public Table eval() {
		LazyExpression w = this.argExp;
		TopKTable r = TopKTable.factory(this.schema(), this.k);
		Table ri = this.intermediateTableProvider.apply(this.argExp.schema());
		Table n = this.intermediateTableProvider.apply(this.argExp.schema());
		
		Record record = w.next();
		while (record != null) {			
			Optional<Record> o = ri.findNoRank(record);
			if(o.isEmpty() || o.get().rank < record.rank) {
				try {
					n.insert(record);
					this.incrementCounter();
					if(o.isPresent()) {
						ri.delete(o.get());
					}
					ri.insert(record);
					this.incrementCounter();
					if(r.size() <= this.k || record.rank >= r.minRank()) {
						LazyExpression le = this.transformation.apply(record);
						Record transformedRecord = le.next();
						while(transformedRecord != null) {
							Optional<Record> to = r.findNoRank(transformedRecord);
							if(to.isEmpty() || to.get().rank < transformedRecord.rank) {
								if(to.isPresent()) {
									r.delete(to.get());
								}
								r.insert(transformedRecord);
								this.incrementCounter();
							}						
							transformedRecord = le.next();
						}
					}
				}
				catch(TableRecordSchemaMismatch e) {
					//Unlikely
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
	public Schema schema() {
		return this.transformation.apply(Record.empty(this.argExp.schema())).schema();
	}
}
