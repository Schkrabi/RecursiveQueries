package rq.common.estimations;

import java.util.ArrayList;
import java.util.Map;
import java.util.function.BiFunction;

import rq.common.statistic.RankHistogram;
import rq.common.statistic.SampledHistogram;
import rq.common.util.Pair;

public class ParetWeightedEstimation_sampledHist implements IEstimation {

	public final int slices;
	public final BiFunction<Object, Object, Double> similarity;
	public final int numOfConsideredValues;
	public final SampledHistogram hist;
	
	public ParetWeightedEstimation_sampledHist(
			int resultSlices,
			BiFunction<Object, Object, Double> similarity,
			int numOfConsideredValues,
			SampledHistogram hist) {
		this.slices = resultSlices;
		this.similarity = similarity;
		this.numOfConsideredValues = numOfConsideredValues;
		this.hist = hist;
	}

	@Override
	public RankHistogram estimate() {
		var count = (double)this.hist.valuesCount();
		var vlsByFreq = this.hist.valuesByFrequency();
		
		var wghtAvg = new ArrayList<Pair<RankHistogram, Double>>();
		for(int i = 0; i < Math.min(this.numOfConsideredValues, vlsByFreq.size()); i++) {
			var c = vlsByFreq.get(i);
			var rhist = this.singleConstHist(c);
			var weight = ((double) this.hist.getCount(c)) / count;
			wghtAvg.add(Pair.of(rhist, weight));
		}
		
		var rslt = RankHistogram.weightedAvg(wghtAvg);
		
		return rslt;
	}

	private RankHistogram singleConstHist(double c) {
		var rslt = new RankHistogram(this.slices);
		
		for(var e : this.hist.getHistogram().entrySet()) {
			var rank = this.similarity.apply(c, e.getKey());
			rslt.addIntervalValue(rslt.fit(rank), e.getValue());
		}
		
		return rslt;
	}
	
	/** Ratio of how many values were used for estimation.
	 * E.g. If 20 of 100 values were used, the ration is 0.2*/
	public double ratioOfUsedValues() {
		var ratio = Math.min(Integer.valueOf(this.hist.valuesByFrequency().size()).doubleValue(),
							Integer.valueOf(this.numOfConsideredValues).doubleValue())
				/ Integer.valueOf(this.hist.valuesByFrequency().size());
		return ratio;
	}
	
	public static RankHistogram estimate(
			int resultSlices,
			BiFunction<Object, Object, Double> similarity,
			int numOfConsideredValues,
			SampledHistogram hist) {
		var est = new ParetWeightedEstimation_sampledHist(resultSlices, similarity, numOfConsideredValues, hist);
		var rslt = est.estimate();
		return rslt;
	}

	@Override
	public String filename() {
		return new StringBuilder()
				.append(".WParet.")
				.append(".")
				.append(this.slices)
				.append(".")
				.append(this.numOfConsideredValues)
				.toString();
	}

	@Override
	public String signature() {
		return "wghPars";
	}

	@Override
	public String paramStr() {
		return new StringBuilder()
				.append("slcs=")
				.append(this.slices)
				.append(".vls=")
				.append(this.numOfConsideredValues)
				.append("att=")
				.append(this.hist.observed.name)
				.toString();
	}
	
	@Override
	public Map<String, String> _params() {
		return Map.of("slcs", Integer.toString(this.slices),
				"vls", Integer.toString(this.numOfConsideredValues),
				"att", this.hist.observed.name);
	}

	@Override
	public int getSlices() {
		return this.slices;
	}
}
