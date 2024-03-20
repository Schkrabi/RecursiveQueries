package rq.common.estimations;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.function.BiFunction;

import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.DataSlicedHistogram.Interval;
import rq.common.statistic.SlicedStatistic.RankInterval;

public abstract class IntervalEstimationCenter extends AbstractIntervalEstimation {

	public IntervalEstimationCenter(int slices, BiFunction<Object, Object, Double> similarity) {
		super(slices, similarity);
	}

	@Override
	protected RankHistogram universalRanks(Set<RankInterval> rankIntervals, DataSlicedHistogram dataIntervals) {
		var histograms = new HashMap<Interval, RankHistogram>();

		var min = dataIntervals.intervals().stream().mapToDouble(i -> i.from).min().getAsDouble();
		var max = dataIntervals.intervals().stream().mapToDouble(i -> i.to).max().getAsDouble();
		
		var center = this.center(min, max);
		
		for (var dataInterval : dataIntervals.intervals()) {
			int count = dataIntervals.get(dataInterval);
			var histogram = new RankHistogram(rankIntervals);
			histogram.addRanks(this.ranksForInterval(dataInterval, count, center));
			histograms.put(dataInterval, histogram);
		}

		var totalRange = max - min;
		var avg = new LinkedHashMap<RankInterval, Double>();

		for (var dataInterval : dataIntervals.intervals()) {
			var histogram = histograms.get(dataInterval);
			var share = (dataInterval.to - dataInterval.from) / totalRange;

			for (var rankInterval : rankIntervals) {
				var count = histogram.get(rankInterval) * share;
				avg.merge(rankInterval, count, (a, b) -> a + b);
			}
		}
		return new RankHistogram(avg);
	}
}
