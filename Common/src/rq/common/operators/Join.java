package rq.common.operators;

import rq.common.table.Schema;
import rq.common.table.Table;
import rq.common.table.TabularExpression;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.HashSet;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.ComparisonDomainMismatchException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.SchemaNotJoinableException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.table.Attribute;
import rq.common.table.Record;

/**
 * Represents an inner join
 * @author Mgr. R.Skrabal
 *
 */
public class Join implements TabularExpression {
	
	public record AttributePair(Attribute attribute1, Attribute attribute2) {}
	
	private TabularExpression argument1;
	private TabularExpression argument2;
	private List<AttributePair> onClause;
	private BiFunction<Double, Double, Double> product;
	
	private Join(
			TabularExpression argument1, 
			TabularExpression argument2, 
			Collection<AttributePair> onClause,
			BiFunction<Double, Double, Double> product) {
		this.argument1 = argument1;
		this.argument2 = argument2;
		this.onClause = new ArrayList<AttributePair>(onClause);
		this.product = product;
	}
	
	/**
	 * Factory method
	 * @param argument1
	 * @param argument2
	 * @param onClause
	 * @return
	 * @throws AttributeNotInSchemaException
	 * @throws ComparisonDomainMismatchException 
	 */
	public static Join factory(
			TabularExpression argument1, 
			TabularExpression argument2, 
			Collection<AttributePair> onClause,
			BiFunction<Double, Double, Double> product) 
		throws AttributeNotInSchemaException, SchemaNotJoinableException, ComparisonDomainMismatchException {
		Schema schema1 = argument1.schema();
		Schema schema2 = argument2.schema();
		for(AttributePair p : onClause) {
			if(!schema1.contains(p.attribute1)) {
				throw new AttributeNotInSchemaException(p.attribute1, schema1);
			}
			if(!schema2.contains(p.attribute2)) {
				throw new AttributeNotInSchemaException(p.attribute2, schema2);
			}
			if(!p.attribute1.domain.equals(p.attribute2.domain)) {
				throw new ComparisonDomainMismatchException(p.attribute1, p.attribute2);
			}
		}
		
		if(!schema1.isJoinableWith(schema2)) {
			throw new SchemaNotJoinableException(schema1, schema2);
		}
		
		return new Join(argument1, argument2, onClause, product);
	}
	
	/**
	 * Factory method
	 * @param argument1
	 * @param argument2
	 * @param product
	 * @param onClauses
	 * @return
	 * @throws AttributeNotInSchemaException
	 * @throws SchemaNotJoinableException
	 * @throws ComparisonDomainMismatchException 
	 */
	public static Join factory(
			TabularExpression argument1, 
			TabularExpression argument2, 
			BiFunction<Double, Double, Double> product,
			AttributePair ...onClauses) 
		throws AttributeNotInSchemaException, SchemaNotJoinableException, ComparisonDomainMismatchException {
		return Join.factory(
				argument1, 
				argument2, 
				Arrays.asList(onClauses),
				product);
	}
	
	/**
	 * Returns true if given records satisfies the on clause. Returns false otherwise
	 * @param record1 
	 * @param record2
	 * @return true or false
	 */
	private boolean isOnClauseSatisfied(Record record1, Record record2) {
		return this.onClause.stream()
				.allMatch(attPair -> {
					try {
						return record1.get(attPair.attribute1).equals(record2.get(attPair.attribute2));
					} catch (AttributeNotInSchemaException e) {
						//Unlikely
						throw new RuntimeException(e);
					}
				});
	}
	
	/**
	 * Joins two reocrds on given schema
	 * @param schema schema the records are joinded on
	 * @param record1
	 * @param record2
	 * @return Record instance
	 */
	private Record joinRecords(Schema schema, Record record1, Record record2) {
		try {
			return Record.factory(
					schema, 
					schema.stream()
						.map(a -> {
							Object o = record1.getNoThrow(a);
							if(o != null) {
								return new Record.AttributeValuePair(a, o);
							}
							return new Record.AttributeValuePair(a, record2.getNoThrow(a));
						})
						.collect(Collectors.toList()), 
					this.product.apply(record1.rank, record2.rank));
		} catch (TypeSchemaMismatchException | AttributeNotInSchemaException e) {
			// Unlikely
			throw new RuntimeException(e);
		}
	}

	@Override
	public Table eval() {
		Schema schema = this.schema();
		Table table = new Table(schema);
		
		for(Record record1 : this.argument1.eval()) {
			for(Record record2 : this.argument2.eval()) {
				if(this.isOnClauseSatisfied(record1, record2)) {
					Record record = this.joinRecords(schema, record1, record2);
					try {
						table.insert(record);
					} catch (TableRecordSchemaMismatch e) {
						//Unlikely
						throw new RuntimeException(e);
					}
				}
			}
		}
		
		return table;
	}

	@Override
	public Schema schema() {
		Set<Attribute> s = new HashSet<Attribute>();
		s.addAll(this.argument1.schema().attributeSet());
		s.addAll(this.argument2.schema().attributeSet());
		Schema schema = null;
		try {
			schema = Schema.factory(s);
		}catch(DuplicateAttributeNameException e) {
			//Unlikely
			throw new RuntimeException(e);
		}
		return schema;
	}

}
