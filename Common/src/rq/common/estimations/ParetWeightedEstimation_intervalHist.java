package rq.common.estimations;

import java.util.ArrayList;
import java.util.Map;
import java.util.function.BiFunction;

import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.DataSlicedHistogram.Interval;
import rq.common.statistic.RankHistogram;
import rq.common.util.Pair;

public class ParetWeightedEstimation_intervalHist implements IEstimation {

	public final int slices;
	public final BiFunction<Object, Object, Double> similarity;
	public final int numOfConsideredValues;
	public final DataSlicedHistogram hist;
	private final RepresentativeProvider representativeProvider;
	
	public ParetWeightedEstimation_intervalHist(
			int slices,
			BiFunction<Object, Object, Double> similarity,
			int numOfConsideredValues,
			DataSlicedHistogram hist,
			RepresentativeProvider representativeProvider) {
		this.slices = slices;
		this.similarity = similarity;
		this.numOfConsideredValues = numOfConsideredValues;
		this.hist = hist;
		this.representativeProvider = representativeProvider;
	}

	@Override
	public String signature() {
		return "wgpPari";
	}

	@Override
	public RankHistogram estimate() {
		var count = (double)this.hist.totalSize();
		var vlsByFreq = this.hist.valuesByFrequency();
		
		var wghtAvg = new ArrayList<Pair<RankHistogram, Double>>();
		for(int i = 0; i < Math.min(this.numOfConsideredValues, vlsByFreq.size()); i++) {
			var c = vlsByFreq.get(i);
			var rhist = this.singleConstHist(c);
			var weight = ((double) this.hist.get(c)) / count;
			wghtAvg.add(Pair.of(rhist, weight));
		}
		
		var rslt = RankHistogram.weightedAvg(wghtAvg);
		
		return rslt;
	}
	
	private RankHistogram singleConstHist(Interval i) {
		var rslt = new RankHistogram(this.slices);
		var c = this.representativeProvider.representative(i);
		
		for(var e : this.hist.data().entrySet()) {
			var rep = this.representativeProvider.representative(e.getKey());
			var rank = this.similarity.apply(c, rep);
			rslt.addIntervalValue(rslt.fit(rank), e.getValue());
		}
		
		return rslt;
	}

	@Override
	public int getSlices() {
		return this.slices;
	}

	@Override
	public Map<String, String> _params() {
		return Map.of(
				"slcs", Integer.toString(this.slices),
				"vls", Integer.toString(this.numOfConsideredValues),
				"att", this.hist.observed.name,
				"int", Integer.toString(this.hist.n));
	}

}
