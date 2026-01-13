package rq.common.estimations;

import java.util.Map;

import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
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
		return Map.of("K", this.representative.toString());
	}

	@Override
	public T representative(Interval interval) {
		return this.representative;
	}

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
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
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
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
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
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
		return est;
	}
}
