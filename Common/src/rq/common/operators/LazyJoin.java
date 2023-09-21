package rq.common.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BinaryOperator;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.LazyIterator;
import rq.common.interfaces.SchemaProvider;
import rq.common.onOperators.OnOperator;
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
		this.leftArg = leftArg;
		this.rightArg = rightArg;
		this.onClause = new ArrayList<OnOperator>(onClause);
		this.product = product;
		this.infimum = infimum;
		this.leftProjection = leftProjection;
		this.rightProjection = rightProjection;
		this.schema = schema;
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
		throws AttributeNotInSchemaException {
		Schema leftSchema = ((SchemaProvider)leftArg).schema();
		Schema rightSchema = ((SchemaProvider)rightArg).schema();
		for(OnOperator p : onClause) {
			if(!leftSchema.contains(p.left)) {
				throw new AttributeNotInSchemaException(p.left, leftSchema);
			}
			if(!rightSchema.contains(p.right)) {
				throw new AttributeNotInSchemaException(p.right, rightSchema);
			}
		}
		
		Set<Attribute> intersection = new HashSet<Attribute>(leftSchema.attributeSet());
		intersection.retainAll(rightSchema.attributeSet());
		
		java.util.Map<Attribute, Attribute> leftProjection = makeProjection(leftSchema, intersection, "left.");
		java.util.Map<Attribute, Attribute> rightProjection = makeProjection(rightSchema, intersection, "right.");
		
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
				BinaryOperator<Double> product,
				BinaryOperator<Double> infimum,
				OnOperator... ons) 
			throws AttributeNotInSchemaException {
		return LazyJoin.factory(
				leftArg,
				rightArg,
				Arrays.asList(ons),
				product,
				infimum);
		
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
}
