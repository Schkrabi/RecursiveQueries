/**
 * 
 */
package rq.common.operators;

import java.util.Optional;
import java.util.function.BiFunction;

import rq.common.exceptions.SchemaNotEqualException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.TabularExpression;
import rq.common.latices.LaticeFactory;
import rq.common.statistic.Statistics;
import rq.common.table.MemoryTable;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.interfaces.Table;

/**
 * @author Mgr. R.Skrabal
 *
 */
public class Intersection implements TabularExpression {
	private final TabularExpression argument1, argument2;
	private final BiFunction<Double, Double, Double> infimum;
	private final BiFunction<Schema, Integer, Table> tableSupplier;
	
	private Intersection(
			TabularExpression argument1, 
			TabularExpression argument2,
			BiFunction<Double, Double, Double> infimum,
			BiFunction<Schema, Integer, Table> tableSupplier) {
		this.argument1 = argument1;
		this.argument2 = argument2;
		this.infimum = infimum;
		this.tableSupplier = tableSupplier;
	}
	
	/**
	 * Factory method
	 * @param argument1
	 * @param argument2
	 * @return Intersection instance
	 * @throws SchemaNotEqualException
	 */
	public static Intersection factory(
			TabularExpression argument1, 
			TabularExpression argument2,
			BiFunction<Double, Double, Double> infimum,
			BiFunction<Schema, Integer, Table> tableSupplier)
		throws SchemaNotEqualException {
		Schema schema1 = argument1.schema();
		Schema schema2 = argument2.schema(); 
		if(!schema1.equals(schema2)) {
			throw new SchemaNotEqualException(schema1, schema2);
		}
		return new Intersection(argument1, argument2, infimum, tableSupplier);
	}
	
	public static Intersection factory(
			TabularExpression argument1, 
			TabularExpression argument2,
			BiFunction<Double, Double, Double> infimum)
		throws SchemaNotEqualException{
		return Intersection.factory(argument1, argument2, infimum, (Schema s, Integer capacity) -> new MemoryTable(s));
	}
	
	public static Intersection factory(
			TabularExpression argument1,
			TabularExpression argument2)
		throws SchemaNotEqualException {
			return Intersection.factory(argument1, argument2, LaticeFactory.instance().getInfimum());
	}
	
	private static class RecordPair {
		public final Record r1, r2;
		
		public RecordPair(rq.common.table.Record r1, rq.common.table.Record r2) {
			this.r1 = r1;
			this.r2 = r2;			
		}
	}

	@Override
	public Table eval() {
		Table table2 = this.argument2.eval();
		Table table = this.tableSupplier.apply(this.schema(), table2.size());
		
		this.argument1.eval().stream()
			.map(r -> {
				Optional<rq.common.table.Record> o = table2.findNoRank(r);
				if(o.isEmpty()) {
					return null;
				}
				return new RecordPair(r, o.get());
			})
			.filter(p -> p != null)
			.forEach(p -> {
				try {
					Record r1 = p.r1;
					Record r2 = p.r2;
					table.insert(new Record(r1, this.infimum.apply(r1.rank, r2.rank)));
				} catch (TableRecordSchemaMismatch e) {
					// Unlikely
					throw new RuntimeException(e);
				}
			});
		return table;
	}

	@Override
	public Schema schema() {
		return this.argument1.schema();
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("(")
				.append(this.argument1.toString())
				.append(") /\\ (")
				.append(this.argument2.toString())
				.append(")")
				.toString();
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
