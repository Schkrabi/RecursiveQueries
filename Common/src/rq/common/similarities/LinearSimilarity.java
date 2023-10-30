package rq.common.similarities;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.time.LocalDateTime;

import rq.common.types.DateTime;

public class LinearSimilarity {

	private static Function<Double, Double> similarityFunction(double similarUntil) {
		return (Double distance) -> Math.max(0.0d, Math.min(1.0d, (-1.0d / similarUntil) * distance + 1));
	}

	public static final BiFunction<Object, Object, Double> integerSimilarityUntil(int similarUntil) {
		return (Object i1, Object i2) -> Math.max(0.0d, Math.min(1.0d,
				similarityFunction(similarUntil).apply(Math.abs((double) ((Integer) i1 - (Integer) i2)))));
	}

	public static final BiFunction<Object, Object, Double> doubleSimilarityUntil(double similarUntil) {
		return (Object d1, Object d2) -> Math.max(0.0d,
				Math.min(1.0d, similarityFunction(similarUntil).apply(Math.abs((Double) d1 - (Double) d2))));
	}

	public static final BiFunction<Object, Object, Double> dateTimeSimilarityUntil(long l) {
		final Function<Double, Double> sFun = similarityFunction(l);
		return (Object t1, Object t2) -> 
			{
				LocalDateTime ldt1 = ((DateTime)t1).getInner();
				LocalDateTime ldt2 = ((DateTime)t2).getInner();
				Duration d = Duration.between(ldt1, ldt2);
				double unrestricted = sFun.apply((double) Math.abs(d.toSeconds()));
				return unrestricted;
			};				
	}

}
