package rq.common.estimations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.DataSlicedHistogram.Interval;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SlicedStatistic.RankInterval;

public abstract class AbstractIntervalEstimation {

	public final int slices;
	protected final BiFunction<Object, Object, Double> similarity;
	
	public AbstractIntervalEstimation(int slices, BiFunction<Object, Object, Double> similarity) {
		this.slices = slices;
		this.similarity = similarity;
	}
	
	public abstract RankHistogram doEstimate();

	protected RankHistogram universalRanks(Set<RankInterval> rankIntervals, DataSlicedHistogram dataIntervals) {
		var histograms = new HashMap<Interval, RankHistogram>();

		var min = dataIntervals.intervals().stream().mapToDouble(i -> i.from).min().getAsDouble();
		var max = dataIntervals.intervals().stream().mapToDouble(i -> i.to).max().getAsDouble();

		for (var dataInterval : dataIntervals.intervals()) {
			int count = dataIntervals.get(dataInterval);
			var center = dataInterval.from + (dataInterval.to - dataInterval.from / 2);
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

	protected List<Double> ranksForInterval(Interval dataInterval, int count, Double value) {
		List<Double> result = new ArrayList<Double>();

		double step = (dataInterval.to - dataInterval.from) / count;
		double x;
		if (value.doubleValue() <= dataInterval.from) {
			x = dataInterval.from;
		} else {
			x = dataInterval.to;
			if (!dataInterval.closedTo)
				x -= step;
			step = -step;
		}

		for (int i = 0; i < count; i++) {
			double rank = this.similarity.apply(x, value.doubleValue());
			if (rank == 0)
				break;
			result.add(rank);
			x += step;
		}

		return result;
	}
}
