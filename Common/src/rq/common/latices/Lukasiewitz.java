package rq.common.latices;

import java.util.function.BinaryOperator;

public class Lukasiewitz {

	public static final BinaryOperator<Double> PRODUCT = (Double x, Double y) -> Math.max(x + y - 1.0d, 0d);
	public static final BinaryOperator<Double> RESIDUUM = (Double x, Double y) -> Math.min(1.0d - x + y, 1.0d);
	
	public static final BinaryOperator<Double> INFIMUM = (Double x, Double y) -> Math.min(x, y);
	public static final BinaryOperator<Double> SUPREMUM = (Double x, Double y) -> Math.max(x, y);
}
