package rq.common.restrictions;

import java.util.function.BinaryOperator;

import rq.common.latices.LaticeFactory;

public class ProductAnd extends BiLogicalCondition {
	
	public ProductAnd(SelectionCondition left, SelectionCondition right) {
		super(left, right, LaticeFactory.instance().getProduct());
	}
	
	public ProductAnd(SelectionCondition left, SelectionCondition right, BinaryOperator<Double> product) {
		super(left, right, product);
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append(this.left)
				.append(" (x) ")
				.append(this.right)
				.toString();
	}

}
