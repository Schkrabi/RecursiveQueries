/**
 * 
 */
package rq.common.restrictions;

import java.util.function.BinaryOperator;

import rq.common.latices.LaticeFactory;

/**
 * 
 */
public class Or extends BiLogicalCondition {

	/**
	 * @param left
	 * @param right
	 * @param product
	 */
	public Or(SelectionCondition left, SelectionCondition right, BinaryOperator<Double> supremum) {
		super(left, right, supremum);
	}

	public Or(SelectionCondition left, SelectionCondition right) {
		super(left, right, LaticeFactory.instance().getSupremum());
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append(this.left)
				.append(" \\/ ")
				.append(this.right)
				.toString();
	}
}
