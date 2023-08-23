package rq.common.latices;

import java.util.function.BinaryOperator;

public class Godel {
	public static final BinaryOperator<Double> PRODUCT = (Double x, Double y) -> Math.min(x, y);
	public static final BinaryOperator<Double> RESIDUUM = (Double x, Double y) -> x <= y ? 1.0d : y;
	
	public static final BinaryOperator<Double> INFIMUM = (Double x, Double y) -> Math.min(x, y);
	public static final BinaryOperator<Double> SUPREMUM = (Double x, Double y) -> Math.max(x, y);
}
