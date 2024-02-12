/**
 * 
 */
package rq.common.operators;

import java.util.function.BinaryOperator;

import rq.common.exceptions.SelectionConditionNotApplicableToSchema;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.SchemaProvider;
import rq.common.latices.LaticeFactory;
import rq.common.restrictions.SelectionCondition;
import rq.common.statistic.Statistics;
import rq.common.table.Record;
import rq.common.table.Schema;

/**
 * Class representing pipelined restriction with structured condition
 */
public class LazySelection extends AbstractSelection implements LazyExpression, SchemaProvider {

	private final LazyExpression argExp;
	private final SchemaProvider argSch;
	
	public <T extends LazyExpression & SchemaProvider> LazySelection(
			T arg,
			SelectionCondition condition,
			BinaryOperator<Double> product) {
		super(condition, product);
		if(!condition.isApplicableToSchema(arg.schema())) {
			throw new RuntimeException(new SelectionConditionNotApplicableToSchema(condition, arg.schema()));
		}
		this.argExp = arg;
		this.argSch = arg;
	}
	
	public <T extends LazyExpression & SchemaProvider> LazySelection(
			T arg,
			SelectionCondition condition){
		super(condition, LaticeFactory.instance().getProduct());
		if(!condition.isApplicableToSchema(arg.schema())) {
			throw new RuntimeException(new SelectionConditionNotApplicableToSchema(condition, arg.schema()));
		}
		this.argExp = arg;
		this.argSch = arg;
	}

	@Override
	public Schema schema() {
		return this.argSch.schema();
	}

	@Override
	public Record next() {
		Record record = this.argExp.next();
		
		while(record != null) {
			double rank = this.recordSatisfaction(record);
			if(rank > 0.0d) {
				return new Record(record, rank);
			}
			record = this.argExp.next();
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
