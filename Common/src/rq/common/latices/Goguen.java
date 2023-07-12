package rq.common.latices;

import java.util.function.BiFunction;

public class Goguen {
	public static final BiFunction<Double, Double, Double> PRODUCT = (Double x, Double y) -> x*y;
	public static final BiFunction<Double, Double, Double> RESIDUUM = (Double x, Double y) -> x < y ? 1.0d : y/x; 
}
