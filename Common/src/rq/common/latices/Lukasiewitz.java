package rq.common.latices;

import java.util.function.BiFunction;

public class Lukasiewitz {

	public static final BiFunction<Double, Double, Double> PRODUCT = (Double x, Double y) -> Math.max(x + y - 1.0d, 0d);
	public static final BiFunction<Double, Double, Double> RESIDUUM = (Double x, Double y) -> Math.min(1.0d - x + y, 1.0d);
}
