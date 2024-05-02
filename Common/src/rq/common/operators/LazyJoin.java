package rq.common.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BinaryOperator;

import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.LazyIterator;
import rq.common.interfaces.SchemaProvider;
import rq.common.latices.LaticeFactory;
import rq.common.onOperators.Constant;
import rq.common.onOperators.OnOperator;
import rq.common.statistic.Statistics;
import rq.common.table.Attribute;
import rq.common.table.CachedExpression;
import rq.common.table.Record;
import rq.common.table.Schema;

/**
 * Represents lazy join operator
 * @author Mgr. Radomir Skrabal
 *
 */
public class LazyJoin extends AbstractJoin implements LazyExpression, SchemaProvider {
	
	private final LazyExpression leftArg;
	private final CachedExpression rightArg;
	
	private final LazyIterator rightIterator;
	private Record leftCurrent;
	
	private LazyJoin(
			LazyExpression leftArg,
			CachedExpression rightArg,
			Collection<OnOperator> onClause,
			BinaryOperator<Double> product,
			BinaryOperator<Double> infimum,
			java.util.Map<Attribute, Attribute> leftProjection,
			java.util.Map<Attribute, Attribute> rightProjection,
			Schema schema) {
		super(new ArrayList<OnOperator>(onClause), product, infimum, leftProjection, rightProjection, schema);
		this.leftArg = leftArg;
		this.rightArg = rightArg;
		this.rightIterator = this.rightArg.lazyIterator();
		this.leftCurrent = null;
	}

	public static <T extends LazyExpression & SchemaProvider, U extends LazyExpression & SchemaProvider>
		LazyJoin factory(
				T leftArg,
				U rightArg,
				Collection<OnOperator> onClause,
				BinaryOperator<Double> product,
				BinaryOperator<Double> infimum)
		throws OnOperatornNotApplicableToSchemaException {
		Schema leftSchema = ((SchemaProvider)leftArg).schema();
		Schema rightSchema = ((SchemaProvider)rightArg).schema();
		for(OnOperator p : onClause) {
			if(!p.isApplicableToSchema(leftSchema, rightSchema)) {
				throw new OnOperatornNotApplicableToSchemaException(p, leftSchema, rightSchema);
			}
		}
		
		Set<Attribute> intersection = new HashSet<Attribute>(leftSchema.attributeSet());
		intersection.retainAll(rightSchema.attributeSet());
		
		java.util.Map<Attribute, Attribute> leftProjection = makeProjection(leftSchema, intersection, LEFT);
		java.util.Map<Attribute, Attribute> rightProjection = makeProjection(rightSchema, intersection, RIGHT);
		
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
		
		return new LazyJoin(
				leftArg,
				CachedExpression.factory(rightArg),
				onClause,
				product,
				infimum,
				leftProjection,
				rightProjection,
				schema);
	}
	
	public static <T extends LazyExpression & SchemaProvider, U extends LazyExpression & SchemaProvider>
	LazyJoin factory(
			T leftArg,
			U rightArg,
			Collection<OnOperator> onClause)
	throws OnOperatornNotApplicableToSchemaException {
		return LazyJoin.factory(
				leftArg, 
				rightArg, 
				onClause,
				LaticeFactory.instance().getProduct(),
				LaticeFactory.instance().getInfimum());
	}
	
	public static <T extends LazyExpression & SchemaProvider, U extends LazyExpression & SchemaProvider>
		LazyJoin factory(
				T leftArg,
				U rightArg,
				BinaryOperator<Double> product,
				BinaryOperator<Double> infimum,
				OnOperator... ons) 
			throws OnOperatornNotApplicableToSchemaException {
		return LazyJoin.factory(
				leftArg,
				rightArg,
				Arrays.asList(ons),
				product,
				infimum);
	}
	
	public static <T extends LazyExpression & SchemaProvider, U extends LazyExpression & SchemaProvider>
	LazyJoin factory(
			T leftArg,
			U rightArg,
			OnOperator... ons) {
		try {
		return LazyJoin.factory(
				leftArg, 
				rightArg, 
				Arrays.asList(ons),
				LaticeFactory.instance().getProduct(),
				LaticeFactory.instance().getInfimum());
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public Schema schema() {
		return this.schema;
	}

	@Override
	public Record next() {
		if(leftCurrent == null) {
			leftCurrent = this.leftArg.next();
		}
		
		while(this.leftCurrent != null) {
			Record right = this.rightIterator.next();
			while(right != null) {
				double clauseRank = this.joinClauseSatisfyDegree(this.leftCurrent, right);
				if(clauseRank > 0.0d) {
					double rank = this.recordRank(this.leftCurrent.rank, right.rank, clauseRank);
					if(rank > 0.0d) {
						Record record = this.joinRecords(this.leftCurrent, right, rank);
						return record;
					}
				}
				right = this.rightIterator.next();
			}
			this.leftCurrent = this.leftArg.next();
			this.rightIterator.restart();
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
	
	/** Crossjoin */
	public static <T extends LazyExpression & SchemaProvider, U extends LazyExpression & SchemaProvider> 
		LazyJoin crossJoin(T left, U right) {
		return LazyJoin.factory(
				left, 
				right, 
				new rq.common.onOperators.OnEquals(new Constant<Boolean>(true), new Constant<Boolean>(true)));
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("(")
				.append(this.leftArg.toString())
				.append(") JOIN (")
				.append(this.rightArg.toString())
				.append(") WHERE ")
				.append(this.onClause.stream().map(p -> p.toString()).reduce((s1, s2) -> s1 + " /\\ " + s2))
				.toString();
	}
}
