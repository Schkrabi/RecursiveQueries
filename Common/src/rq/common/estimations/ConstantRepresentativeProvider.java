package rq.common.estimations;

import java.util.Map;
import java.util.function.BiFunction;

import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.DataSlicedHistogram.Interval;

public class ConstantRepresentativeProvider implements RepresentativeProvider {
	public final double representative;
	
	public ConstantRepresentativeProvider(double representative) {
		this.representative = representative;
	}

	@Override
	public String signature() {
		return "K";
	}
	
	@Override
	public Map<String, String> params() {
		return Map.of("K", Double.toString(this.representative));
	}

	@Override
	public double representative(Interval interval) {
		return this.representative;
	}

	public static IntervalEstimation eqdK(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquidistantHistogram hist,
			double representative) {
		var est = new IntervalEstimation(
				slices, 
				similarity,
				hist,
				new ConstantRepresentativeProvider(representative),
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
		return est;
	}
	
	public static IntervalEstimation eqnK(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquinominalHistogram hist,
			double representative) {
		var est = new IntervalEstimation(
				slices, 
				similarity,
				hist,
				new ConstantRepresentativeProvider(representative),
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
		return est;
	}
	
	public static IntervalEstimation fromHist(int slices, 
			BiFunction<Object, Object, Double> similarity,
			DataSlicedHistogram hist,
			double representative) {
		var est = new IntervalEstimation(
				slices, 
				similarity,
				hist,
				new ConstantRepresentativeProvider(representative),
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
		return est;
	}
}
