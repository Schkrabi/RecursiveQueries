package rq.common.operators;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.onOperators.OnOperator;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Record.AttributeValuePair;
import rq.common.table.Schema;

public class AbstractJoin {
	
	private static Attribute aliasAttribute(String prefix, Attribute a) {
		return new Attribute(prefix + a.name, a.domain);
	}
	
	protected final static String LEFT = "left.";
	protected final static String RIGHT = "right.";
	
	/**
	 * Aliases the attribute as left part of a join
	 * @param a
	 * @return
	 */
	public static Attribute left(Attribute a) {
		return aliasAttribute(LEFT, a);
	}
	/**
	 * Aliases the attribute as right part of the join
	 * @param a
	 * @return
	 */
	public static Attribute right(Attribute a) {
		return aliasAttribute(RIGHT, a);
	}

	/**
	 * Creates a projection of attributes to the joined table attributes
	 * @param schema projected schema
	 * @param intersection intersection of the attributes
	 * @param tableAlias prefix in the joined table
	 * @return a mapping from schema attributes to new joined table attributes
	 */
	protected static java.util.Map<Attribute, Attribute> makeProjection(Schema schema, Set<Attribute> intersection, String tableAlias) {
		java.util.Map<Attribute, Attribute> m = new HashMap<Attribute, Attribute>();
		schema.stream()
			.filter(a -> !intersection.contains(a))
			.forEach(a -> m.put(a, a));		
		intersection.stream()
			.forEach(a -> m.put(a, new Attribute(tableAlias + a.name, a.domain)));
		return m;
	}

	protected final List<OnOperator> onClause;
	protected final BinaryOperator<Double> product;
	protected final BinaryOperator<Double> infimum;
	protected final java.util.Map<Attribute, Attribute> leftProjection;
	protected final java.util.Map<Attribute, Attribute> rightProjection;
	protected final Schema schema;
	
	protected AbstractJoin(List<OnOperator> onClause,
			BinaryOperator<Double> product,
			BinaryOperator<Double> infimum,
			java.util.Map<Attribute, Attribute> leftProjection,
			java.util.Map<Attribute, Attribute> rightProjection,
			Schema schema) {
		this.onClause = onClause;
		this.product = product;
		this.infimum = infimum;
		this.leftProjection = leftProjection;
		this.rightProjection = rightProjection;
		this.schema = schema;
	}

	/**
	 * Returns degree to which the ON clause is satisfied
	 * @param record1 left joined record
	 * @param record2 right joined record
	 * @return A degree
	 */
	protected double joinClauseSatisfyDegree(Record record1, Record record2) {
		double rank = 1.0d;
		
		for(OnOperator clause : this.onClause) {
			double clauseRank = clause.eval(record1, record2);
			rank = this.infimum.apply(rank, clauseRank);
		}
		return rank;
	}
	
	/**
	 * Computes final rank of the joined record
	 * @param leftRank
	 * @param rightRank
	 * @param clauseRank
	 * @return rank
	 */
	protected double recordRank(double leftRank, double rightRank, double clauseRank) {
		return this.product.apply(leftRank, this.product.apply(rightRank, clauseRank));
	}

	/**
	 * Joins two reocrds on given schema
	 * @param schema schema the records are joinded on
	 * @param record1
	 * @param record2
	 * @return Record instance
	 */
	protected Record joinRecords(Record record1, Record record2, Double rank) {
		try {
			Collection<AttributeValuePair> vls = record1.schema.stream()
															.map((Attribute a) -> new AttributeValuePair(this.leftProjection.get(a), record1.getNoThrow(a)))
															.collect(Collectors.toList());
			record2.schema.stream()
				.map(a -> new AttributeValuePair(this.rightProjection.get(a), record2.getNoThrow(a)))
				.forEach(p -> vls.add(p));
			
			return Record.factory(
					this.schema, 
					vls, 
					rank);
		} catch (TypeSchemaMismatchException | AttributeNotInSchemaException e) {
			// Unlikely
			throw new RuntimeException(e);
		}
	}

}
