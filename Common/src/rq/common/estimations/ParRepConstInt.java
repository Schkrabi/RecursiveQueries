package rq.common.estimations;

import java.util.Map;

import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.RankHistogram;

public class ParRepConstInt implements IEstimation {

	public final int slices;
	public final double constant;
	public final rq.common.similarities.ISimilarity<Double> similarity;
	private final DataSlicedHistogram<Double> hist;
	public final int numOfConsideredValues;
	private final RepresentativeProvider<Double> representativeProvider;
	
	public ParRepConstInt(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity, 
			DataSlicedHistogram<Double> hist,
			double constant,
			int numOfConsideredValues,
			RepresentativeProvider<Double> representativeProvider) {
		this.slices = slices;
		this.similarity = similarity;
		this.hist = hist;
		this.constant = constant;
		this.numOfConsideredValues = numOfConsideredValues;
		this.representativeProvider = representativeProvider;
	}

	@Override
	public String signature() {
		return "Prci";
	}

	@Override
	public RankHistogram estimate() {
		var orderedValues = this.hist.valuesByFrequency();
		
		var hist = new RankHistogram(this.slices);
		for(int i = 0; i < Math.min(this.numOfConsideredValues, orderedValues.size()); i++) {
			var intrvl = orderedValues.get(i);
			var value = this.representativeProvider.representative(intrvl);
			var rank = this.similarity.apply(value, this.constant);
			hist.addIntervalValue(hist.fit(rank), this.hist.get(intrvl));
		}
		
		return hist;
	}

	@Override
	public int getSlices() {
		return this.slices;
	}

	@Override
	public Map<String, String> _params() {
		return Map.of("slcs", Integer.toString(this.slices),
				"c", Double.toString(this.constant),
				"att", this.hist.observed.name,
				"int", Integer.toString(this.hist.n),
				"vls", Integer.toString(this.numOfConsideredValues));
	}

}
