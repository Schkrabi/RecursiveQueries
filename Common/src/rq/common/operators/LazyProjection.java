/**
 * 
 */
package rq.common.operators;

import java.util.Arrays;
import java.util.Collection;
import java.util.PriorityQueue;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.NotSubschemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.SchemaProvider;
import rq.common.latices.LaticeFactory;
import rq.common.onOperators.RecordValue;
import rq.common.operators.Projection.To;
import rq.common.statistic.Statistics;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;

/**
 * Represents lazy projection operation
 * 
 * @author Mgr. Radomir Skrabal
 *
 */
public class LazyProjection implements LazyExpression, SchemaProvider {
	
	private final Schema schema;
	private final java.util.Map<Attribute, RecordValue> projection;
	private final LazyExpression argExp;
	private final BinaryOperator<Double> supremum;
	
	private PriorityQueue<Record> queue = null;
	
	private LazyProjection(Schema schema, java.util.Map<Attribute, RecordValue> projection, LazyExpression argExp, SchemaProvider argSch, BinaryOperator<Double> supremum) {
		this.schema = schema;
		this.projection = projection;
		this.argExp = argExp;
		this.supremum = supremum;
	}
	
	/**
	 * Factory method
	 * @param <T>
	 * @param argument
	 * @param schema
	 * @return
	 * @throws NotSubschemaException
	 */
	public static <T extends LazyExpression & SchemaProvider> LazyProjection factory(T argument, Schema schema)
			throws NotSubschemaException {
		SchemaProvider argSch = (SchemaProvider) argument;
		if (!argSch.schema().isSubSchema(schema)) {
			throw new NotSubschemaException(argSch.schema(), schema);
		}

		java.util.Map<Attribute, RecordValue> projection = new java.util.HashMap<Attribute, RecordValue>();
		schema.stream().forEach(a -> projection.put(a, a));

		return new LazyProjection(schema, projection, argument, argument, LaticeFactory.instance().getSupremum());
	}
	
	/**
	 * Factory method
	 * @param <T>
	 * @param argument
	 * @param mapping
	 * @return
	 * @throws AttributeNotInSchemaException
	 * @throws DuplicateAttributeNameException
	 * @throws RecordValueNotApplicableOnSchemaException 
	 */
	public static <T extends LazyExpression & SchemaProvider> LazyProjection factory(T argument, Collection<To> mapping) 
			throws DuplicateAttributeNameException, RecordValueNotApplicableOnSchemaException {
		Schema fromSchema = ((SchemaProvider)argument).schema();
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
		
		return new LazyProjection(schema, projection, argument, argument, LaticeFactory.instance().getSupremum());
	}
	
	public static <T extends LazyExpression & SchemaProvider> LazyProjection factory(T argument, To... tos)
			throws DuplicateAttributeNameException, RecordValueNotApplicableOnSchemaException {
		return LazyProjection.factory(argument, Arrays.asList(tos));
	}

	@Override
	public Schema schema() {
		return this.schema;
	}
	
	private Record project(Record record) {
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
			throw new RuntimeException(e);
		}
	}

	@Override
	public Record next() {
		if(this.queue == null) {
			var arg = LazyExpression.realizeInMemory(this.argExp);
			if(arg != null) {
				this.queue = new PriorityQueue<Record>(Math.max(arg.size(), 1));
				arg.stream().forEach(r -> this.queue.add(r));
			}
		}
		
		if(this.queue != null) {
			Record record = this.queue.poll();
			if(record == null) {
				return null;
			}
			var p = this.project(record);
			if(this.queue.isEmpty()) {
				return p;
			}
			
			var p1 = this.project(this.queue.peek());
			while(p.equalsNoRank(p1)) {
				p = new Record(p, this.supremum.apply(p.rank, p1.rank));
				this.queue.poll();
				p1 = this.project(this.queue.peek());
			}
			
			return p;
		}
		return null;
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
