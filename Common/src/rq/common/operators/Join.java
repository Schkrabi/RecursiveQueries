package rq.common.operators;

import rq.common.table.Schema;
import rq.common.table.MemoryTable;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.HashSet;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.ComparisonDomainMismatchException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.SchemaNotJoinableException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.TabularExpression;
import rq.common.onOperators.OnOperator;
import rq.common.table.Attribute;
import rq.common.table.Record;

/**
 * Represents an inner join
 * @author Mgr. R.Skrabal
 *
 */
public class Join extends AbstractJoin implements TabularExpression {
	
	public class AttributePair 
	{
		public final Attribute attribute1, attribute2;
		
		public AttributePair(Attribute attribute1, Attribute attribute2) {
			this.attribute1 = attribute1;
			this.attribute2 = attribute2;			
		}
		
	}
	
	private TabularExpression argument1;
	private TabularExpression argument2;
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
	
	@Override
	public MemoryTable eval() {
		Schema schema = this.schema();
		MemoryTable table = new MemoryTable(schema);
		
		for(Record record1 : this.argument1.eval()) {
			for(Record record2 : this.argument2.eval()) {
				Double onClauseSatisfyDegree = this.joinClauseSatisfyDegree(record1, record2);
				if(onClauseSatisfyDegree > 0.0d) {
					double rank = this.recordRank(record1.rank, record2.rank, onClauseSatisfyDegree);
					if(rank > 0.0d) {
						Record record = this.joinRecords(record1, record2, rank);
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
