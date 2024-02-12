/**
 * 
 */
package rq.common.restrictions;

import java.util.function.BinaryOperator;

import rq.common.latices.LaticeFactory;

/**
 * 
 */
public class InfimumAnd extends BiLogicalCondition {

	/**
	 * @param left
	 * @param right
	 * @param product
	 */
	public InfimumAnd(SelectionCondition left, SelectionCondition right, BinaryOperator<Double> product) {
		super(left, right, product);
	}
	
	public InfimumAnd(SelectionCondition left, SelectionCondition right) {
		super(left, right, LaticeFactory.instance().getInfimum());
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append(this.left)
				.append(" /\\ ")
				.append(this.right)
				.toString();
	}

}
