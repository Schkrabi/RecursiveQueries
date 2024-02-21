package rq.common.estimations;

import java.util.LinkedHashMap;
import java.util.function.BinaryOperator;

import rq.common.statistic.RankHistogram;
import rq.common.statistic.SlicedStatistic.RankInterval;

/** Estimates the crossjoin operation */
public class EstimateCrossJoin {

	private final RankHistogram left;
	private final RankHistogram right;
	private final BinaryOperator<Double> product;
	
	public EstimateCrossJoin(
			RankHistogram left,
			RankHistogram right,
			BinaryOperator<Double> product) {
		if(!left.getSlices().equals(right.getSlices())) {
			throw new RuntimeException("Must have the same slices!");
		}
		this.left = left;
		this.right = right;
		this.product = product;
	}
	
	/** Estimation logic */
	public RankHistogram doEstimate() {
		var values = new LinkedHashMap<RankInterval, Double>(); 
		
		//Compute the crossjoin histogram
		for(RankInterval lint : this.left.getSlices()) {
			for(RankInterval rint : this.right.getSlices()) {
				double start = this.product.apply(lint.start, rint.start);
				double end = this.product.apply(lint.end, rint.end);
				double value = this.left.get(lint) * this.right.get(rint);
				
				values.put(new RankInterval(start, end), value);
			}
		}		
		
		//Recalculate it for final histogram
		var rslt = new LinkedHashMap<RankInterval, Double>();
		
		for(var e : values.entrySet()) {
			var ratios = e.getKey().ratiosOverMultipleIntervals(this.left.getSlices());
			for(var f : ratios.entrySet()) {
				double value = f.getValue() * e.getValue();
				rslt.merge(f.getKey(), value, (x, y) -> x + y);
			}
		}
		
		return new RankHistogram(rslt);
	}

	/** Estimates the crossjoin	 */
	public static RankHistogram estimate(RankHistogram left, RankHistogram right, BinaryOperator<Double> product) {
		var me = new EstimateCrossJoin(left, right, product);
		return me.doEstimate();
	}
}
