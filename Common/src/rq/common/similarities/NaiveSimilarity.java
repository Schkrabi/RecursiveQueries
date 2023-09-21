package rq.common.similarities;

import java.time.Duration;
import java.util.function.BiFunction;
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
			(Object i1, Object i2) -> Math.max((-K * Math.abs((double)((Integer)i1 - (Integer)i2))) + 1.0d, 0.0d);
			
	public static final BiFunction<Object, Object, Double> DOUBLE_SIMILARITY =
			(Object d1, Object d2) -> Math.max((-K * Math.abs((Double)d1 - (Double)d2)) + 1.0d, 0.0d);
			
	public static final BiFunction<Object, Object, Double> DATETIME_SIMILARITY =
			(Object t1, Object t2) -> Math.max((-K * Duration.between((LocalDateTime)t1, (LocalDateTime)t2).toMinutes()) , 0.0d);
			
}
