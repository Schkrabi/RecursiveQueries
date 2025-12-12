package rq.common.estimations;

import java.util.Map;
import java.util.function.BiFunction;

import rq.common.statistic.RankHistogram;
import rq.common.statistic.SampledHistogram;

/**
 * Precisely estimates 
 */
public class ParRepConstSampl implements IEstimation {

    public final int resultSlices;
	public final double constant;
	public final BiFunction<Object, Object, Double> similarity;
	private SampledHistogram hist;
	public final int numOfConsideredValues;
	
	public ParRepConstSampl(
			int slices, 
			BiFunction<Object, Object, Double> similarity, 
			SampledHistogram hist,
			double constant,
			int numOfConsideredValues) {
		this.hist = hist;
		this.similarity = similarity;
		this.resultSlices = slices;
		this.constant = constant;
		this.numOfConsideredValues = numOfConsideredValues;
	}
	
	public ParRepConstSampl(
			int slices, 
			BiFunction<Object, Object, Double> similarity, 
			SampledHistogram hist,
			double constant) {
		this.hist = hist;
		this.similarity = similarity;
		this.resultSlices = slices;
		this.constant = constant;
		this.numOfConsideredValues = 20;

	}

	public RankHistogram estimate() {
		var orderedValues = this.hist.valuesByFrequency();
		
		var hist = new RankHistogram(this.resultSlices);
		for(int i = 0; i < Math.min(this.numOfConsideredValues, orderedValues.size()); i++) {
			var value = orderedValues.get(i);
			var rank = this.similarity.apply(value, this.constant);
			hist.addIntervalValue(hist.fit(rank), this.hist.getCount(value));
		}
		
		return hist;
	}
	
	/** Ratio of how many values were used for estimation.
	 * E.g. If 20 of 100 values were used, the ration is 0.2*/
	public double ratioOfUsedValues() {		
		var ratio = Math.min(Integer.valueOf(this.hist.valuesByFrequency().size()).doubleValue(),
							Integer.valueOf(this.numOfConsideredValues).doubleValue())
				/ Integer.valueOf(this.hist.valuesByFrequency().size()).doubleValue();
		return ratio;
	}

	@Override
	public String signature() {
		return "Prcs";
	}

	@Override
	public int getSlices() {
		return this.resultSlices;
	}

	@Override
	public Map<String, String> _params() {
		return Map.of("slc", Integer.toString(this.resultSlices),
				"c", Double.toString(this.constant),
				"att", this.hist.observed.name,
				"vls", Integer.toString(numOfConsideredValues));
	}

}
