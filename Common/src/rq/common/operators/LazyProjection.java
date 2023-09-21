/**
 * 
 */
package rq.common.operators;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.NotSubschemaException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.SchemaProvider;
import rq.common.operators.Projection.To;
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
	private final java.util.Map<Attribute, Attribute> projection;
	private final LazyExpression argExp;
	private LazyProjection(Schema schema, java.util.Map<Attribute, Attribute> projection, LazyExpression argExp, SchemaProvider argSch) {
		this.schema = schema;
		this.projection = projection;
		this.argExp = argExp;
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

		java.util.Map<Attribute, Attribute> projection = new java.util.HashMap<Attribute, Attribute>();
		schema.stream().forEach(a -> projection.put(a, a));

		return new LazyProjection(schema, projection, argument, argument);
	}
	
	/**
	 * Factory method
	 * @param <T>
	 * @param argument
	 * @param mapping
	 * @return
	 * @throws AttributeNotInSchemaException
	 * @throws DuplicateAttributeNameException
	 */
	public static <T extends LazyExpression & SchemaProvider> LazyProjection factory(T argument, Collection<To> mapping) 
			throws AttributeNotInSchemaException, DuplicateAttributeNameException {
		Schema fromSchema = ((SchemaProvider)argument).schema();
		for(To to : mapping) {
			if(!fromSchema.contains(to.from)) {
				throw new AttributeNotInSchemaException(to.from, fromSchema);
			}
		}
		
		Schema schema = Schema.factory(
				mapping.stream()
				.map(to -> to.to)
				.collect(Collectors.toList()));
		
		java.util.Map<Attribute, Attribute> projection = new java.util.HashMap<Attribute, Attribute>();
		mapping.stream().forEach(t -> projection.put(t.to, t.from));
		
		return new LazyProjection(schema, projection, argument, argument);
	}
	
	public static <T extends LazyExpression & SchemaProvider> LazyProjection factory(T argument, To... tos)
			throws AttributeNotInSchemaException, DuplicateAttributeNameException {
		return LazyProjection.factory(argument, Arrays.asList(tos));
	}

	@Override
	public Schema schema() {
		return this.schema;
	}

	@Override
	public Record next() {
		Record record = this.argExp.next();
		if(record == null) {
			return null;
		}
		try {
			return 
				Record.factory(
						this.schema,
						this.schema.stream()
							.map(a -> {
								try {
									return new Record.AttributeValuePair(
											a, 
											record.get(this.projection.get(a)));
								} catch (AttributeNotInSchemaException e) {
									// Unlikely
									throw new RuntimeException(e);
								}
							}).collect(Collectors.toList()),
						record.rank);
		} catch (TypeSchemaMismatchException | AttributeNotInSchemaException e) {
			// Unlikely
			throw new RuntimeException(e);
		}
	}

}
