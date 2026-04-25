package rq.common.estimations;

import java.util.Collection;
import java.util.Map;

import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.util.Pair;
import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.DataSlicedHistogram.Interval;
import rq.common.similarities.ISimilarity;

public class ConstantRepresentativeProvider<T> implements RepresentativeProvider<T> {
	public final T representative;
	
	public ConstantRepresentativeProvider(T representative) {
		this.representative = representative;
	}

	@Override
	public String signature() {
		return "K";
	}
	
	@Override
	public Map<String, String> params() {
		return Map.of("c", this.representative.toString());
	}

	@Override
	public T representative(Interval interval) {
		return this.representative;
	}
	
	public static IntervalEstimation.AggregationProvider SUM_AGGREGATION_PROVIDER = 
			new IntervalEstimation.AggregationProvider() {

				@Override
				public String signature() {
					return "";
				}

				@Override
				public RankHistogram aggregate(Collection<Pair<RankHistogram, Double>> histsAndRatios) {
					return histsAndRatios.stream()
						.map(p -> p.first)
						.reduce((h1, h2) -> RankHistogram.add(h1, h2))
						.get();
				}
		
	};

	public static IntervalEstimation<Double> eqdK(
			int slices, 
			ISimilarity<Double> similarity,
			EquidistantHistogram<Double> hist,
			double representative) {
		var est = new IntervalEstimation<>(
				slices, 
				similarity,
				hist,
				new ConstantRepresentativeProvider<>(representative),
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER,
				SUM_AGGREGATION_PROVIDER);
		return est;
	}
	
	public static IntervalEstimation<Double> eqnK(
			int slices, 
			ISimilarity<Double> similarity,
			EquinominalHistogram<Double> hist,
			double representative) {
		var est = new IntervalEstimation<>(
				slices, 
				similarity,
				hist,
				new ConstantRepresentativeProvider<>(representative),
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER,
				SUM_AGGREGATION_PROVIDER);
		return est;
	}
	
	public static IntervalEstimation<Double> fromHist(int slices, 
			ISimilarity<Double> similarity,
			DataSlicedHistogram<Double> hist,
			double representative) {
		var est = new IntervalEstimation<>(
				slices, 
				similarity,
				hist,
				new ConstantRepresentativeProvider<>(representative),
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER,
				SUM_AGGREGATION_PROVIDER);
		return est;
	}
}
