package rq.common.estimations;

import java.util.Map;
import java.util.function.BiFunction;

import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.DataSlicedHistogram.Interval;

public class GlobalCenterRepresentativeProvider implements RepresentativeProvider {

	private final DataSlicedHistogram hist;
	
	public GlobalCenterRepresentativeProvider(
			DataSlicedHistogram hist) {
		this.hist = hist;
	}
	
	@Override
	public String signature() {
		return "C";
	}

	private Double _globalCenter = null;
	@Override
	public double representative(Interval interval) {
		if(_globalCenter == null) {
			var d = Math.abs(this.hist.max() - this.hist.min());
			_globalCenter = this.hist.min() + d/2;
		}
		return _globalCenter;
	}

	public static IntervalEstimation eqdC(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquidistantHistogram hist) {
		var est = new IntervalEstimation(
				slices, 
				similarity,
				hist,
				new GlobalCenterRepresentativeProvider(hist),
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
		return est;
	}
	
	public static IntervalEstimation eqnC(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquinominalHistogram hist) {
		var est = new IntervalEstimation(
				slices, 
				similarity,
				hist,
				new GlobalCenterRepresentativeProvider(hist),
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
		return est;
	}

	@Override
	public Map<String, String> params() {
		return Map.of();
	}
}
