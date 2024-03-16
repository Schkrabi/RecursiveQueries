/**
 * 
 */
package rq.common.operators;

import java.util.Optional;
import java.util.function.BiFunction;

import rq.common.exceptions.SchemaNotEqualException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.latices.LaticeFactory;
import rq.common.statistic.Statistics;
import rq.common.table.Schema;
import rq.common.table.MemoryTable;
import rq.common.table.Record;

/**
 * Represents union of two tabular expressions
 * @author Mgr. R.Skrabal
 *
 */
public class Union implements TabularExpression {
	private final TabularExpression argument1;
	private final TabularExpression argument2;
	private final BiFunction<Double, Double, Double> supremum;
	private final BiFunction<Schema, Integer, Table> tableSupplier;
	
	private Union(
			TabularExpression argument1, 
			TabularExpression argument2,
			BiFunction<Double, Double, Double> supremum,
			BiFunction<Schema, Integer, Table> tableSupplier) {
		this.argument1 = argument1;
		this.argument2 = argument2;
		this.supremum = supremum;
		this.tableSupplier = tableSupplier;
	}
	
	/**
	 * Factory method
	 * @param argument1 first argument of union
	 * @param argument2 second argument of union
	 * @return Union instance
	 * @throws SchemaNotEqualException if argument schemas are not equal
	 */
	public static Union factory(
			TabularExpression argument1, 
			TabularExpression argument2,
			BiFunction<Double, Double, Double> supremum,
			BiFunction<Schema, Integer, Table> tableSupplier) 
		throws SchemaNotEqualException {
		Schema schema1 = argument1.schema();
		Schema schema2 = argument2.schema(); 
		if(!schema1.equals(schema2)) {
			throw new SchemaNotEqualException(schema1, schema2);
		}
		return new Union(argument1, argument2, supremum, tableSupplier);
	}
	
	public static Union factory(
			TabularExpression argument1, 
			TabularExpression argument2,
			BiFunction<Double, Double, Double> supremum)
		throws SchemaNotEqualException {
		return Union.factory(
				argument1, 
				argument2, 
				supremum,
				(Schema s, Integer count) -> new MemoryTable(s));
	}
	
	public static Union factory(
			TabularExpression argument1, 
			TabularExpression argument2)
		throws SchemaNotEqualException {
		return Union.factory(
				argument1, 
				argument2,
				LaticeFactory.instance().getSupremum());
	}

	@Override
	public Table eval() {
		Table arg = this.argument1.eval(); 
		var arg2 = this.argument2.eval(); 
		Table table = this.tableSupplier.apply(this.schema(), arg.size() + arg2.size());
		arg.stream().forEach(r -> {
			try {
				table.insert(r);
			} catch (TableRecordSchemaMismatch e) {
				// Unlikely
				throw new RuntimeException(e);
			}
		});
		arg2.stream().forEach(r -> {
			try {
				Optional<Record> o = table.findNoRank(r);
				if(o.isEmpty()) {
					table.insert(r);
				}
				else {
					table.delete(o.get());
					Record n = new Record(r, this.supremum.apply(o.get().rank, r.rank));
					table.insert(n);
				}
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
				.append("UNION(")
				.append(this.argument1.toString())
				.append(", ")
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
