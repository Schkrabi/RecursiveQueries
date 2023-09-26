/**
 * 
 */
package rq.common.operators;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.NotSubschemaException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.interfaces.TabularExpression;
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
	private final java.util.Map<Attribute, Attribute> projection;
	private final BiFunction<Schema, Integer, Table> tableSupplier;
	
	private Projection(TabularExpression argument, Schema schema, java.util.Map<Attribute, Attribute> projection, BiFunction<Schema, Integer, Table> tableSupplier) {
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
		java.util.Map<Attribute, Attribute> projection = new java.util.HashMap<Attribute, Attribute>();
		schema.stream().forEach(a -> projection.put(a, a));
		
		return new Projection(argument, schema, projection, tableSupplier);
	}
	
	public static class To{
		public final Attribute from, to;
		
		public To(Attribute from, Attribute to) {
			this.from = from;
			this.to = to;
		}
	}
	
	public static Projection factory(TabularExpression table, Collection<To> mapping)
			throws AttributeNotInSchemaException, DuplicateAttributeNameException {
		return Projection.factory(table, mapping, (Schema s, Integer count) -> new MemoryTable(s));
	}
	
	/**
	 * Factory method
	 * @param table
	 * @param mapping
	 * @return
	 * @throws AttributeNotInSchemaException
	 * @throws DuplicateAttributeNameException
	 */
	public static Projection factory(TabularExpression table, Collection<To> mapping, BiFunction<Schema, Integer, Table> tableSupplier) 
		throws AttributeNotInSchemaException, DuplicateAttributeNameException {
		Schema fromSchema = table.schema();
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
		
		return new Projection(table, schema, projection, tableSupplier);
	}
	
	public static Projection factory(TabularExpression argument, To... mapping) 
			throws AttributeNotInSchemaException, DuplicateAttributeNameException {
		return Projection.factory(argument, Arrays.asList(mapping), (Schema s, Integer count) -> new MemoryTable(s));
	}
	
	/**
	 * Factory method
	 * @param argument
	 * @param mapping
	 * @return
	 * @throws AttributeNotInSchemaException
	 * @throws DuplicateAttributeNameException
	 */
	public static Projection factory(TabularExpression argument, BiFunction<Schema, Integer, Table> tableSupplier, To... mapping) 
			throws AttributeNotInSchemaException, DuplicateAttributeNameException {
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

	@Override
	public Table eval() {
		Table source = this.argument.eval();
		Table dest = this.tableSupplier.apply(schema, source.size());
		
		for(Record r : source) {
			try {
				dest.insert(this.project(r));
			} catch (TableRecordSchemaMismatch | TypeSchemaMismatchException e) {
				// Unlikely
				return null;
			}
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
				.append("PROJECT(")
				.append(this.argument)
				.append(")")
				.append(" TO ")
				.append(this.schema.toString())
				.toString();
	}
}
