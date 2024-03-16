package rq.common.estimations;

import java.util.LinkedHashMap;

import rq.common.statistic.RankHistogram;
import rq.common.statistic.SlicedStatistic.RankInterval;

/** Estimate for union operation*/
public class EstimateUnion {

	/** Histogram of left operand */
	private final RankHistogram left;
	/** Histogram of the right operand */
	private final RankHistogram right;
	
	public EstimateUnion(
			RankHistogram left,
			RankHistogram right) {
		if(!left.getSlices().equals(right.getSlices())) {
			throw new RuntimeException("Must have the same slices!");
		}
		//Left is always smaller
		if(left.tableSize() < right.tableSize()) {
			this.left = left;
			this.right = right;
		}
		else {
			this.left = right;
			this.right = left;
		}
	}

	/** Estimates */
	public RankHistogram doEstimate() {
		var rsltData = new LinkedHashMap<RankInterval, Double>();
		
		var typeBRows = this.left.tableSize() / (4* this.left.getSlices().size());
		
		for(RankInterval i : this.left.getSlices()) {
			var value = this.left.get(i)
						- typeBRows
						+ (this.right.get(i) / 2);
			
			value = Math.max(0.d, value);
			
			rsltData.put(i, value);
		}
		
		return new RankHistogram(rsltData);
	}
	
	public static RankHistogram estimate(
			RankHistogram left,
			RankHistogram right) {
		EstimateUnion me = new EstimateUnion(left, right);
		return me.doEstimate();
	}
}
