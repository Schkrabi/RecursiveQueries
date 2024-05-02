package rq.common.restrictions;

import java.util.function.BinaryOperator;

import rq.common.table.Record;
import rq.common.table.Schema;

abstract class BiLogicalCondition implements SelectionCondition {
	
	protected final SelectionCondition left;
	protected final SelectionCondition right;
	protected final BinaryOperator<Double> combiner;
	
	protected BiLogicalCondition(SelectionCondition left, SelectionCondition right, BinaryOperator<Double> product) {
		this.left = left;
		this.right = right;
		this.combiner = product;
	}

	@Override
	public boolean isApplicableToSchema(Schema schema) {
		return this.left.isApplicableToSchema(schema)
				&& this.right.isApplicableToSchema(schema);
	}

	@Override
	public double eval(Record record) {
		Double leftValue = this.left.eval(record);
		Double rightValue = this.right.eval(record);
		
		return this.combiner.apply(leftValue, rightValue);
	}
}
