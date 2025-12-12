package rq.common.statistic;

import java.util.Map;
import java.util.Random;

import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.DataSlicedHistogram.Interval;

public class RandRepresentativeProvider implements RepresentativeProvider {

	public final Random rand;
	
	public RandRepresentativeProvider(Random rand) {
		this.rand = rand;
	}
	
	@Override
	public String signature() {
		return "R";
	}

	@Override
	public Map<String, String> params() {
		return Map.of();
	}

	@Override
	public double representative(Interval interval) {
		var d = interval.to - interval.from;
		var rep = interval.from + this.rand.nextDouble() * d;
		return rep;
	}
	
}