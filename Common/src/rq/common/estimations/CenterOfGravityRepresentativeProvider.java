package rq.common.estimations;

import java.util.Map;

import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.DataSlicedHistogram.Interval;
import rq.common.statistic.MostCommonValues;

public class CenterOfGravityRepresentativeProvider implements RepresentativeProvider {

	private final MostCommonValues mcv;
	
	public CenterOfGravityRepresentativeProvider(
			MostCommonValues mcv) {
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
	public double representative(Interval interval) {
		return mcv.centerOfGravity(interval);
	}

}
