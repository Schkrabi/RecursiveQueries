package rq.common.estimations;

import java.util.LinkedHashMap;

import rq.common.statistic.RankHistogram;
import rq.common.statistic.SlicedStatistic.RankInterval;

public class IntersectionEstimation {

	private final RankHistogram left;
	private final RankHistogram right;
	
	public IntersectionEstimation(
			RankHistogram left,
			RankHistogram right) {
		if(!left.getSlices().equals(right.getSlices())) {
			throw new RuntimeException("Must have the same slices.");
		}
		//Left is always the smaller
		if(left.tableSize() < right.tableSize()) {
			this.left = left;
			this.right = right;
		}
		else {
			this.left = right;
			this.right = left;
		}
	}
	
	public RankHistogram doEstimate() {
		var rslt = new LinkedHashMap<RankInterval, Double>();
		
		var lSize = this.left.tableSize();
		var rSize = this.right.tableSize();
		
		for(RankInterval i : this.left.getSlices()) {
			for(RankInterval j : this.right.getSlices()) {
				var interval = new RankInterval(Math.min(i.start, j.start), Math.min(i.end, j.end));
				var value = this.left.get(i)/lSize
						*	lSize/2
						*	this.right.get(j)/rSize;
				rslt.merge(interval, value, (x, y) -> x+y);
			}
		}
		
		return new RankHistogram(rslt);
	}
	
	public static RankHistogram estimate(
			RankHistogram left,
			RankHistogram right) {
		var me = new IntersectionEstimation(left, right);
		var rslt = me.doEstimate();
		return rslt;
	}
}
