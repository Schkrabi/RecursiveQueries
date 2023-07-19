package rq.common.latices;

import java.util.function.BiFunction;

public class Godel {
	public static final BiFunction<Double, Double, Double> PRODUCT = (Double x, Double y) -> Math.min(x, y);
	public static final BiFunction<Double, Double, Double> RESIDUUM = (Double x, Double y) -> x <= y ? 1.0d : y;
	
	public static final BiFunction<Double, Double, Double> INFIMUM = (Double x, Double y) -> Math.min(x, y);
	public static final BiFunction<Double, Double, Double> SUPREMUM = (Double x, Double y) -> Math.max(x, y);
}
