package rq.common.operators;

import java.util.function.BinaryOperator;
import rq.common.restrictions.SelectionCondition;

/**
 * Abstract class for selection operation with structured condition
 */
public class AbstractSelection {

	public final SelectionCondition condition;
	public final BinaryOperator<Double> product;

	public AbstractSelection(SelectionCondition condition, BinaryOperator<Double> product) {
		super();
		this.condition = condition;
		this.product = product;
	}
	
	/**
	 * Rank of the record in the selection
	 * @param record inspected rank
	 * @return rank of the record in the selection
	 */
	protected double recordSatisfaction(rq.common.table.Record record) {
		double condRank = this.condition.eval(record);
		if(condRank > 0.0d) {
			double rank = this.product.apply(record.rank, condRank);
			return rank;
		}
		return 0.0d;
	}

}