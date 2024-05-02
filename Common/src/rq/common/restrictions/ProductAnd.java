package rq.common.restrictions;

import java.util.function.BinaryOperator;
import java.util.stream.Stream;

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
	
	/** Creates and expression for n conditiions */
	public static ProductAnd and(SelectionCondition ...conds) {
		return (ProductAnd) Stream.of(conds).reduce((c1, c2) -> new ProductAnd(c1, c2)).get();		
	}
}
