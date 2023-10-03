package rq.common.similarities;

import java.time.Duration;
import java.util.function.BiFunction;

import rq.common.types.DateTime;

import java.time.LocalDateTime;

/**
 * Class for naive testing similarity functions
 * @author Mgr. R.Skrabal
 *
 */
public class NaiveSimilarity {
	/**
	 * 
	 */
	private static final Double K = 0.75d;
	
	public static final BiFunction<Object, Object, Double> INTEGER_SIMILARITY = 
			(Object i1, Object i2) -> Math.min(1.0d, Math.max((-K * Math.abs((double)((Integer)i1 - (Integer)i2))) + 1.0d, 0.0d));
			
	public static final BiFunction<Object, Object, Double> DOUBLE_SIMILARITY =
			(Object d1, Object d2) -> Math.min(1.0d, Math.max((-K * Math.abs((Double)d1 - (Double)d2)) + 1.0d, 0.0d));
			
	public static final BiFunction<Object, Object, Double> LOCALDATETIME_SIMILARITY_MINUTES =
			(Object t1, Object t2) -> Math.min(1.0d, Math.max((-K * Duration.between((LocalDateTime)t1, (LocalDateTime)t2).toMinutes()) + 1.0d, 0.0d));
			
	public static final BiFunction<Object, Object, Double> DATETIME_SIMILARITY_MINUTES =
			(Object t1, Object t2) -> Math.min(1.0d, Math.max((-K * Duration.between(((DateTime)t1).getInner(), ((DateTime)t2).getInner()).toSeconds()/60.0d) + 1.0d, 0.0d));
			
	public static final BiFunction<Object, Object, Double> DATETIME_SIMILARITY_HOURS =
			(Object t1, Object t2) -> Math.min(1.0d, Math.max((-K * ((double)Duration.between(((DateTime)t1).getInner(), ((DateTime)t2).getInner()).toMinutes()/3600.0d)) + 1.0d, 0.0d));
			
}
