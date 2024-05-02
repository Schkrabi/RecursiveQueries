/**
 * 
 */
package rq.common.operators;

import java.util.Arrays;
import java.util.Collection;
import java.util.PriorityQueue;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.NotSubschemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.interfaces.TabularExpression;
import rq.common.latices.LaticeFactory;
import rq.common.onOperators.RecordValue;
import rq.common.statistic.Statistics;
import rq.common.table.Schema;
import rq.common.table.MemoryTable;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.interfaces.Table;

/**
 * Represents a projection operator
 * @author Mgr. R.Skrabal
 *
 */
public class Projection implements TabularExpression {
	
	private final Schema schema;
	private final TabularExpression argument;
	private final java.util.Map<Attribute, RecordValue> projection;
	private final BiFunction<Schema, Integer, Table> tableSupplier;
	private final BinaryOperator<Double> supremum = LaticeFactory.instance().getSupremum();
	
	private Projection(TabularExpression argument, Schema schema, java.util.Map<Attribute, RecordValue> projection, BiFunction<Schema, Integer, Table> tableSupplier) {
		this.schema = schema;
		this.argument = argument;
		this.projection = projection;
		this.tableSupplier = tableSupplier;
	}
	
	public static Projection factory(TabularExpression argument, Schema schema)
			throws NotSubschemaException{
		return Projection.factory(argument, schema, (Schema s, Integer count) -> new MemoryTable(s));
	}
	
	/**
	 * Factory method
	 * @param argument
	 * @param schema
	 * @return
	 * @throws NotSubschemaException
	 */
	public static Projection factory(TabularExpression argument, Schema schema, BiFunction<Schema, Integer, Table> tableSupplier) 
		throws NotSubschemaException{
		if(!argument.schema().isSubSchema(schema)) {
			throw new NotSubschemaException(argument.schema(), schema);
		}
		java.util.Map<Attribute, RecordValue> projection = new java.util.HashMap<Attribute, RecordValue>();
		schema.stream().forEach(a -> projection.put(a, a));
		
		return new Projection(argument, schema, projection, tableSupplier);
	}
	
	public static class To{
		public final RecordValue from;
		public final Attribute to;
		
		public To(RecordValue from, Attribute to) {
			this.from = from;
			this.to = to;
		}
	}
	
	public static Projection factory(TabularExpression table, Collection<To> mapping)
			throws DuplicateAttributeNameException, RecordValueNotApplicableOnSchemaException {
		return Projection.factory(table, mapping, (Schema s, Integer count) -> new MemoryTable(s));
	}
	
	/**
	 * Factory method
	 * @param table
	 * @param mapping
	 * @return
	 * @throws DuplicateAttributeNameException
	 * @throws RecordValueNotApplicableOnSchemaException 
	 */
	public static Projection factory(TabularExpression table, Collection<To> mapping, BiFunction<Schema, Integer, Table> tableSupplier) 
		throws DuplicateAttributeNameException, RecordValueNotApplicableOnSchemaException {
		Schema fromSchema = table.schema();
		for(To to : mapping) {
			if(!to.from.isApplicableToSchema(fromSchema)) {
				throw new RecordValueNotApplicableOnSchemaException(to.from, fromSchema);
			}
		}
		Schema schema = Schema.factory(
						mapping.stream()
						.map(to -> to.to)
						.collect(Collectors.toList()));
		
		java.util.Map<Attribute, RecordValue> projection = new java.util.HashMap<Attribute, RecordValue>();
		mapping.stream().forEach(t -> projection.put(t.to, t.from));
		
		return new Projection(table, schema, projection, tableSupplier);
	}
	
	public static Projection factory(TabularExpression argument, To... mapping) 
			throws DuplicateAttributeNameException, RecordValueNotApplicableOnSchemaException {
		return Projection.factory(argument, Arrays.asList(mapping), (Schema s, Integer count) -> new MemoryTable(s));
	}
	
	/**
	 * Factory method
	 * @param argument
	 * @param mapping
	 * @return
	 * @throws AttributeNotInSchemaException
	 * @throws DuplicateAttributeNameException
	 * @throws RecordValueNotApplicableOnSchemaException 
	 */
	public static Projection factory(TabularExpression argument, BiFunction<Schema, Integer, Table> tableSupplier, To... mapping) 
			throws DuplicateAttributeNameException, RecordValueNotApplicableOnSchemaException {
		return Projection.factory(argument, Arrays.asList(mapping), tableSupplier);
	}
	
	/**
	 * Projects a single record
	 * @param record
	 * @return
	 * @throws TypeSchemaMismatchException
	 */
	private Record project(Record record) throws TypeSchemaMismatchException {
		try {
			return 
				Record.factory(
						this.schema,
						this.projection.entrySet().stream()
							.map(e -> {
								return new Record.AttributeValuePair(
										e.getKey(), 
										e.getValue().value(record));
							}).collect(Collectors.toList()),
						record.rank);
		} catch (TypeSchemaMismatchException | AttributeNotInSchemaException e) {
			// Unlikely
			throw new RuntimeException(e);
		}
	}

	@Override
	public Table eval() {
		Table source = this.argument.eval();
		var q = new PriorityQueue<Record>(Math.max(source.size(), 1));
		source.stream().forEach(r -> q.add(r));
		
		Table dest = this.tableSupplier.apply(schema, source.size());
		
		try {
			var r = q.poll();
			while(r != null) {
				var p = this.project(r);
				if(!q.isEmpty()) {
					var q1 = q.peek();
					
					if(q1 != null) {
						var p1 = this.project(q.peek());
						
						while(p.equalsNoRank(p1)) {
							p = new Record(p, this.supremum.apply(p.rank, p1.rank));
							q.poll();
							if(q.isEmpty()) {
								break;
							}
							p1 = this.project(q.peek());
						}
					}
				}
				
				dest.insert(p);			
				r = q.poll();
			}
		} catch (TableRecordSchemaMismatch | TypeSchemaMismatchException e) {
			throw new RuntimeException(e);
		}
		
		return dest;
	}

	@Override
	public Schema schema() {
		return this.schema;
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append("[")
				.append(this.projection.entrySet().stream()
						.map(e -> {
							if(e.getKey().equals(e.getValue())) {
								return e.getKey();
							}
							return new StringBuilder()
									.append(e.getValue())
									.append(" AS ")
									.append(e.getKey())
									.toString();
						})
						.reduce((s1, s2) -> new StringBuilder()
											.append(s1)
											.append(", ")
											.append(s2)
											.toString()))
				.append(" FROM (")
				.append(this.argument.toString())
				.append(")]")
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
