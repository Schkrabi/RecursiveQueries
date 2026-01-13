package rq.common.estimations;

import java.util.Map;

import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.DataSlicedHistogram.Interval;

public class GlobalCenterRepresentativeProvider implements RepresentativeProvider<Double> {

	private final DataSlicedHistogram<Double> hist;
	
	public GlobalCenterRepresentativeProvider(
			DataSlicedHistogram<Double> hist) {
		this.hist = hist;
	}
	
	@Override
	public String signature() {
		return "C";
	}

	private Double _globalCenter = null;
	@Override
	public Double representative(Interval interval) {
		if(_globalCenter == null) {
			var d = Math.abs(this.hist.max() - this.hist.min());
			_globalCenter = this.hist.min() + d/2;
		}
		return _globalCenter;
	}

	public static IntervalEstimation<Double> eqdC(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity,
			EquidistantHistogram<Double> hist) {
		var est = new IntervalEstimation<>(
				slices, 
				similarity,
				hist,
				new GlobalCenterRepresentativeProvider(hist),
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
		return est;
	}
	
	public static IntervalEstimation<Double> eqnC(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity,
			EquinominalHistogram<Double> hist) {
		var est = new IntervalEstimation<>(
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
