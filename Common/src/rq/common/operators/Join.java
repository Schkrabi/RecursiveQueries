package rq.common.operators;

import rq.common.table.Schema;
import rq.common.table.MemoryTable;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.HashSet;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.ComparisonDomainMismatchException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.SchemaNotJoinableException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.latices.LaticeFactory;
import rq.common.onOperators.Constant;
import rq.common.onOperators.OnOperator;
import rq.common.statistic.Statistics;
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
	
	private final TabularExpression argument1;
	private final TabularExpression argument2;
	private final BiFunction<Schema, Integer, Table> tableSupplier;
	
	private Join(
			TabularExpression argument1, 
			TabularExpression argument2, 
			Collection<OnOperator> onClause,
			BinaryOperator<Double> product,
			BinaryOperator<Double> infimum,
			java.util.Map<Attribute, Attribute> leftProjection,
			java.util.Map<Attribute, Attribute> rightProjection,
			Schema schema,
			BiFunction<Schema, Integer, Table> tableSupplier) {
		super(new ArrayList<OnOperator>(onClause), product, infimum, leftProjection, rightProjection, schema);
		this.argument1 = argument1;
		this.argument2 = argument2;
		this.tableSupplier = tableSupplier;
	}
	
	/**
	 * Factory method
	 * @param argument1
	 * @param argument2
	 * @param onClause
	 * @return
	 * @throws AttributeNotInSchemaException
	 * @throws OnOperatornNotApplicableToSchemaException 
	 * @throws ComparisonDomainMismatchException 
	 * @throws  
	 */
	public static Join factory(
			TabularExpression argument1, 
			TabularExpression argument2, 
			Collection<OnOperator> onClause,
			BinaryOperator<Double> product,
			BinaryOperator<Double> infimum,
			BiFunction<Schema, Integer, Table> tableSupplier) 
		throws OnOperatornNotApplicableToSchemaException {
		Schema schema1 = argument1.schema();
		Schema schema2 = argument2.schema();
		for(OnOperator p : onClause) {
			if(!p.isApplicableToSchema(schema1, schema2)) {
				throw new OnOperatornNotApplicableToSchemaException(p, schema1, schema2);
			}
		}
		
		Set<Attribute> intersection = new HashSet<Attribute>(schema1.attributeSet());
		intersection.retainAll(schema2.attributeSet());
		
		java.util.Map<Attribute, Attribute> leftProjection = makeProjection(schema1, intersection, LEFT);
		java.util.Map<Attribute, Attribute> rightProjection = makeProjection(schema2, intersection, RIGHT);
		
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
		
		return new Join(argument1, argument2, onClause, product, infimum, leftProjection, rightProjection, schema, tableSupplier);
	}
	
	public static Join factory(
			TabularExpression argument1, 
			TabularExpression argument2, 
			Collection<OnOperator> onClause,
			BinaryOperator<Double> product,
			BinaryOperator<Double> infimum) 
		throws OnOperatornNotApplicableToSchemaException {
		return Join.factory(argument1, argument2, onClause, product, infimum, (Schema s, Integer count) -> new MemoryTable(s));
	}
	
	public static Join factory(
			TabularExpression argument1, 
			TabularExpression argument2, 
			Collection<OnOperator> onClause)
		throws OnOperatornNotApplicableToSchemaException {
		return Join.factory(
				argument1, 
				argument2, 
				onClause, 
				LaticeFactory.instance().getProduct(), 
				LaticeFactory.instance().getInfimum());
	}
	
	/**
	 * Factory method
	 * @param argument1
	 * @param argument2
	 * @param product
	 * @param onClauses
	 * @return
	 * @throws AttributeNotInSchemaException
	 * @throws OnOperatornNotApplicableToSchemaException 
	 * @throws SchemaNotJoinableException
	 * @throws ComparisonDomainMismatchException 
	 */
	public static Join factory(
			TabularExpression argument1, 
			TabularExpression argument2, 
			BinaryOperator<Double> product,
			BinaryOperator<Double> infimum,
			OnOperator ...onClauses) 
		throws OnOperatornNotApplicableToSchemaException {
		return Join.factory(
				argument1, 
				argument2, 
				Arrays.asList(onClauses),
				product,
				infimum);
	}
	
	public static Join factory(
			TabularExpression argument1, 
			TabularExpression argument2,
			OnOperator ...onClauses)
		throws OnOperatornNotApplicableToSchemaException {
		return Join.factory(
				argument1, 
				argument2, 
				Arrays.asList(onClauses),
				LaticeFactory.instance().getProduct(),
				LaticeFactory.instance().getInfimum());
	}
	
	@Override
	public Table eval() {
		Schema schema = this.schema();
		
		Table t1 = this.argument1.eval();
		Table t2 = this.argument2.eval();
		Table table = this.tableSupplier.apply(schema, t1.size() * t2.size());
		
		for(Record record1 : t1) {
			for(Record record2 : t2) {
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
				.append(") JOIN (")
				.append(this.argument2.toString())
				.append(") WHERE ")
				.append(this.onClause.stream().map(p -> p.toString()).reduce((s1, s2) -> s1 + " /\\ " + s2))
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
	
	/** Creates crossjoin */
	public static Join crossJoin(TabularExpression left, TabularExpression right) {
		try {
			return Join.factory(left, right, 
					new rq.common.onOperators.OnEquals(
							new Constant<Boolean>(true), 
							new Constant<Boolean>(true)));
		} catch (OnOperatornNotApplicableToSchemaException e) {
			//unlikely
			throw new RuntimeException(e);
		}
	}
}
