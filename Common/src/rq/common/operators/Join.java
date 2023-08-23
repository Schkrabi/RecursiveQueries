package rq.common.operators;

import rq.common.table.Schema;
import rq.common.table.Table;
import rq.common.table.TabularExpression;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Arrays;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.HashSet;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.ComparisonDomainMismatchException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.SchemaNotJoinableException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.onOperators.OnOperator;
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
	private List<OnOperator> onClause;
	private BinaryOperator<Double> product;
	private BinaryOperator<Double> infimum;
	private java.util.Map<Attribute, Attribute> leftProjection;
	private java.util.Map<Attribute, Attribute> rightProjection;
	private Schema schema;
	
	private Join(
			TabularExpression argument1, 
			TabularExpression argument2, 
			Collection<OnOperator> onClause,
			BinaryOperator<Double> product,
			BinaryOperator<Double> infimum,
			java.util.Map<Attribute, Attribute> leftProjection,
			java.util.Map<Attribute, Attribute> rightProjection,
			Schema schema) {
		this.argument1 = argument1;
		this.argument2 = argument2;
		this.onClause = new ArrayList<OnOperator>(onClause);
		this.product = product;
		this.infimum = infimum;
		this.leftProjection = leftProjection;
		this.rightProjection = rightProjection;
		this.schema = schema;
	}
	
	/**
	 * Creates a projection of attributes to the joined table attributes
	 * @param schema projected schema
	 * @param intersection intersection of the attributes
	 * @param tableAlias prefix in the joined table
	 * @return a mapping from schema attributes to new joined table attributes
	 */
	private static java.util.Map<Attribute, Attribute> makeProjection(Schema schema, Set<Attribute> intersection, String tableAlias) {
		java.util.Map<Attribute, Attribute> m = new HashMap<Attribute, Attribute>();
		schema.stream()
			.filter(a -> !intersection.contains(a))
			.forEach(a -> m.put(a, a));		
		intersection.stream()
			.forEach(a -> m.put(a, new Attribute(tableAlias + a.name, a.domain)));
		return m;
	}
	
	/**
	 * Factory method
	 * @param argument1
	 * @param argument2
	 * @param onClause
	 * @return
	 * @throws AttributeNotInSchemaException
	 * @throws ComparisonDomainMismatchException 
	 * @throws  
	 */
	public static Join factory(
			TabularExpression argument1, 
			TabularExpression argument2, 
			Collection<OnOperator> onClause,
			BinaryOperator<Double> product,
			BinaryOperator<Double> infimum) 
		throws AttributeNotInSchemaException {
		Schema schema1 = argument1.schema();
		Schema schema2 = argument2.schema();
		for(OnOperator p : onClause) {
			if(!schema1.contains(p.left)) {
				throw new AttributeNotInSchemaException(p.left, schema1);
			}
			if(!schema2.contains(p.right)) {
				throw new AttributeNotInSchemaException(p.right, schema2);
			}
		}
		
		Set<Attribute> intersection = new HashSet<Attribute>(schema1.attributeSet());
		intersection.retainAll(schema2.attributeSet());
		
		java.util.Map<Attribute, Attribute> leftProjection = makeProjection(schema1, intersection, "left.");
		java.util.Map<Attribute, Attribute> rightProjection = makeProjection(schema2, intersection, "right.");
		
		List<Attribute> attrs = new ArrayList<Attribute>(leftProjection.size() + rightProjection.size());
		attrs.addAll(leftProjection.values());
		attrs.addAll(rightProjection.values());
		Schema schema = null;
		
		try {
			schema = Schema.factory(attrs);
		} catch (DuplicateAttributeNameException e) {
			//Unlikely
			throw new RuntimeException(e);
		}
		
		return new Join(argument1, argument2, onClause, product, infimum, leftProjection, rightProjection, schema);
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
			BinaryOperator<Double> product,
			BinaryOperator<Double> infimum,
			OnOperator ...onClauses) 
		throws AttributeNotInSchemaException {
		return Join.factory(
				argument1, 
				argument2, 
				Arrays.asList(onClauses),
				product,
				infimum);
	}
	
	/**
	 * Returns degree to which the ON clause is satisfied
	 * @param record1 left joined record
	 * @param record2 right joined record
	 * @return A degree
	 */
	private double joinClauseSatisfyDegree(Record record1, Record record2) {
		Optional<Double> o = this.onClause.stream()
								.map(clause -> clause.eval(record1, record2))
								.reduce(this.infimum);
		//The optional will be empty if OnClause is empty, therefore it is trivially satisfied
		if(o.isEmpty()) {
			return 1.0d;
		}
		return o.get();
	}
	
	/**
	 * Joins two reocrds on given schema
	 * @param schema schema the records are joinded on
	 * @param record1
	 * @param record2
	 * @return Record instance
	 */
	private Record joinRecords(Record record1, Record record2, Double onClauseSatisfyDegree) {
		try {
			Collection<Record.AttributeValuePair> vls = record1.schema.stream()
															.map(a -> new Record.AttributeValuePair(this.leftProjection.get(a), record1.getNoThrow(a)))
															.collect(Collectors.toList());
			record2.schema.stream()
				.map(a -> new Record.AttributeValuePair(this.rightProjection.get(a), record2.getNoThrow(a)))
				.forEach(p -> vls.add(p));
			
			return Record.factory(
					this.schema, 
					vls, 
					this.product.apply(record1.rank, this.product.apply(record2.rank, onClauseSatisfyDegree)));
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
				Double onClauseSatisfyDegree = this.joinClauseSatisfyDegree(record1, record2);
				if(onClauseSatisfyDegree > 0.0d) {
					Record record = this.joinRecords(record1, record2, onClauseSatisfyDegree);
					if(record.rank > 0.0d) {
						try {
							table.insert(record);
						} catch (TableRecordSchemaMismatch e) {
							//Unlikely
							throw new RuntimeException(e);
						}
					}
				}
			}
		}
		
		return table;
	}

	@Override
	public Schema schema() {
		return this.schema;
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append("(")
				.append(this.argument1.toString())
				.append(" JOIN ")
				.append(this.argument2)
				.append(" ON ")
				.append(this.onClause.stream().map(p -> p.toString()).reduce((s1, s2) -> s1 + " AND " + s2))
				.append(")")
				.toString();
	}
}
