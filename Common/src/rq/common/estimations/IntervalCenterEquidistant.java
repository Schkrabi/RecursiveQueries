package rq.common.estimations;

import java.util.function.BiFunction;

import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SlicedStatistic;

public class IntervalCenterEquidistant extends IntervalEstimationCenter {

private final EquidistantHistogram hist;
	
	public IntervalCenterEquidistant(int slices, EquidistantHistogram hist, BiFunction<Object, Object, Double> similarity) {
		super(slices, similarity);
		this.hist = hist;
	}

	@Override
	public RankHistogram doEstimate() {
		var intervals = SlicedStatistic.uniformSlices(this.slices);
		var rslt = this.universalRanks(intervals, this.hist);
		return rslt;
	}
	
	public static RankHistogram estimate(int slices, EquidistantHistogram hist, BiFunction<Object, Object, Double> similarity) {
		var me = new IntervalCenterEquidistant(slices, hist, similarity);
		var rslt = me.doEstimate();
		return rslt;
	}

}
