package rq.common.estimations;

import java.util.Map;

import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.DataSlicedHistogram.Interval;
import rq.common.statistic.MostCommonValues;

public class CenterOfGravityRepresentativeProvider implements RepresentativeProvider<Double> {

	private final MostCommonValues<Double> mcv;
	
	public CenterOfGravityRepresentativeProvider(
			MostCommonValues<Double> mcv) {
		this.mcv = mcv;
	}

	@Override
	public String signature() {
		return "G";
	}

	@Override
	public Map<String, String> params() {
		return Map.of();
	}

	@Override
	public Double representative(Interval interval) {
		return mcv.centerOfGravity(interval);
	}

}
