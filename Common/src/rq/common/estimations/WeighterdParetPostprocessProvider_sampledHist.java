package rq.common.estimations;

import java.util.List;
import java.util.Map;

import rq.common.estimations.IntervalEstimation.GlobalPostprocessProvider;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SampledHistogram;
import rq.common.util.Pair;

public class WeighterdParetPostprocessProvider_sampledHist implements GlobalPostprocessProvider {

	public final int slices;
	public final rq.common.similarities.ISimilarity<Double> similarity;
	public final int numOfConsideredValues;
	public final SampledHistogram<Double> hist;
	public final double paretRatio;
	
	public WeighterdParetPostprocessProvider_sampledHist(
			int slices,
			rq.common.similarities.ISimilarity<Double> similarity,
			int numOfConsideredValues,
			double paretRatio,
			SampledHistogram<Double> hist) {
		this.slices = slices;
		this.similarity = similarity;
		this.numOfConsideredValues = numOfConsideredValues;
		this.hist = hist;
		
		if(paretRatio < 0 || paretRatio > 1.0d) {
			throw new RuntimeException("Paret ratio must be between 0 and 1, got " + Double.toString(paretRatio));
		}
		this.paretRatio = paretRatio;
	}

	@Override
	public String signature() {
		return "Gps";
	}

	@Override
	public RankHistogram postprocess(RankHistogram hist) {
		var est = new ParetWeightedEstimation_sampledHist(
				this.slices,
				this.similarity,
				this.numOfConsideredValues,
				this.hist);
		
		var pHist = est.estimate();
		var rslt = RankHistogram.weightedAvg(List.of(
				Pair.of(hist, 1.0d - this.paretRatio),
				Pair.of(pHist, this.paretRatio)));
		return rslt;
	}
	
	/**
	 * 
	 * @param slices
	 * @param similarity
	 * @param hist eqd histogram - base for estimation
	 * @param numOfConsideredValues - number of values considered for paret ratio
	 * @param paretRatio - weight the most frequent values should have in the estimation
	 * @param sHist sampled histogram - base for the estimation
	 * @return
	 */
	public static IntervalEstimation<Double> eqdGps(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity,
			EquidistantHistogram<Double> hist,
			int numOfConsideredValues,
			double paretRatio,
			SampledHistogram<Double> sHist) {
		var est = new IntervalEstimation<>(
				slices, 
				similarity,
				hist,
				IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER,
				new WeighterdParetPostprocessProvider_sampledHist(slices, similarity, numOfConsideredValues, paretRatio, sHist),
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER,
				IntervalEstimation.WEIGHTED_AVG_AGGREGATION_PROVIDER);
		return est;
	}
	
	/**
	 * 
	 * @param slices
	 * @param similarity
	 * @param hist eqn histogram - base for estimation
	 * @param numOfConsideredValues - number of values considered for paret ratio
	 * @param paretRatio - weight the most frequent values should have in the estimation
	 * @param sHist sampled histogram - base for the estimation
	 * @return
	 */
	public static IntervalEstimation<Double> eqnGps(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity,
			EquinominalHistogram<Double> hist,
			int numOfConsideredValues,
			double paretRatio,
			SampledHistogram<Double> sHist) {
		var est = new IntervalEstimation<>(
				slices, 
				similarity,
				hist,
				IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER,
				new WeighterdParetPostprocessProvider_sampledHist(slices, similarity, numOfConsideredValues, paretRatio, sHist),
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER,
				IntervalEstimation.WEIGHTED_AVG_AGGREGATION_PROVIDER);
		return est;
	}
	

	/**
	 * 
	 * @param slices
	 * @param similarity
	 * @param hist eqd histogram - base for estimation
	 * @param numOfConsideredValues - number of values considered for paret ratio
	 * @param paretRatio - weight the most frequent values should have in the estimation
	 * @param sHist sampled histogram - base for the estimation
	 * @return
	 */
	public static IntervalEstimation<Double> eqdCGps(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity,
			EquidistantHistogram<Double> hist,
			int numOfConsideredValues,
			double paretRatio,
			SampledHistogram<Double> sHist) {
		var est = new IntervalEstimation<>(
				slices, 
				similarity,
				hist,
				new GlobalCenterRepresentativeProvider(hist),
				new WeighterdParetPostprocessProvider_sampledHist(slices, similarity, numOfConsideredValues, paretRatio, sHist),
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER,
				IntervalEstimation.WEIGHTED_AVG_AGGREGATION_PROVIDER);
		return est;
	}
	
	/**
	 * 
	 * @param slices
	 * @param similarity
	 * @param hist eqd histogram - base for estimation
	 * @param numOfConsideredValues - number of values considered for paret ratio
	 * @param paretRatio - weight the most frequent values should have in the estimation
	 * @param sHist sampled histogram - base for the estimation
	 * @return
	 */
	public static IntervalEstimation<Double> eqnCGps(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity,
			EquinominalHistogram<Double> hist,
			int numOfConsideredValues,
			double paretRatio,
			SampledHistogram<Double> sHist) {
		var est = new IntervalEstimation<>(
				slices, 
				similarity,
				hist,
				new GlobalCenterRepresentativeProvider(hist),
				new WeighterdParetPostprocessProvider_sampledHist(slices, similarity, numOfConsideredValues, paretRatio, sHist),
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER,
				IntervalEstimation.WEIGHTED_AVG_AGGREGATION_PROVIDER);
		return est;
	}
	
	@Override
	public Map<String, String> params() {
		return Map.of("vls", Integer.toString(numOfConsideredValues),
				"pr", Double.toString(this.paretRatio));
	}
}
