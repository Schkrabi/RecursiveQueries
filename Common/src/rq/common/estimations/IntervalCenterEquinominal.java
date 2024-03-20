package rq.common.estimations;

import java.util.function.BiFunction;

import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SlicedStatistic;

public class IntervalCenterEquinominal extends IntervalEstimationCenter {

	private final EquinominalHistogram hist;

	public IntervalCenterEquinominal(int slices, EquinominalHistogram hist, BiFunction<Object, Object, Double> similarity) {
		super(slices, similarity);	
		this.hist = hist;
		
	}

	@Override
	public RankHistogram doEstimate() {
		var intervals = SlicedStatistic.uniformSlices(this.slices);
		var rslt = this.universalRanks(intervals, this.hist);
		return rslt;
	}

	public static RankHistogram estimate(int slices, EquinominalHistogram hist,
			BiFunction<Object, Object, Double> similarity) {
		var me = new IntervalCenterEquinominal(slices, hist, similarity);
		var rslt = me.doEstimate();
		return rslt;
	}

}
