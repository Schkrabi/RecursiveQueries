package rq.common.estimations;

import java.util.function.BiFunction;

import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SlicedStatistic;

public class IntervalEquinominal extends rq.common.estimations.AbstractIntervalEstimation{

	
	private final EquinominalHistogram hist;

	public IntervalEquinominal(int slices, EquinominalHistogram hist, BiFunction<Object, Object, Double> similarity) {
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
		var me = new IntervalEquinominal(slices, hist, similarity);
		var rslt = me.doEstimate();
		return rslt;
	}

}
