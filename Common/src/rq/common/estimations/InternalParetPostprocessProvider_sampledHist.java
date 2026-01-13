package rq.common.estimations;

import java.util.List;
import java.util.Map;

import rq.common.estimations.IntervalEstimation.IntervalPostprocessProvider;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SampledHistogram;
import rq.common.util.Pair;

public class InternalParetPostprocessProvider_sampledHist implements IntervalPostprocessProvider<Double> {

	public final int slices;
	public final rq.common.similarities.ISimilarity<Double> similarity;
	private SampledHistogram<Double> hist;
	public final int numOfConsideredValues;
	public final double paretRatio;
	
	public InternalParetPostprocessProvider_sampledHist(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity, 
			SampledHistogram<Double> hist,
			int numOfConsideredValues,
			double paretRatio) {
		this.hist = hist;
		this.similarity = similarity;
		this.slices = slices;
		this.numOfConsideredValues = numOfConsideredValues;
		
		if(paretRatio < 0 || paretRatio > 1) {
			throw new RuntimeException("Paret ratio must be between 0 and 1, got " + paretRatio);
		}
		this.paretRatio = paretRatio;
	}

	@Override
	public String signature() {
		return "Ips";
	}

	@Override
	public RankHistogram postprocess(RankHistogram hist, int count, Double representative) {
		var est = new ParRepConstSampl<>(
				this.slices, 
				this.similarity, 
				this.hist, 
				representative,
				this.numOfConsideredValues);
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
	public static IntervalEstimation<Double> eqdIps(
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
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				new InternalParetPostprocessProvider_sampledHist(slices, similarity, sHist, numOfConsideredValues, paretRatio));
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
	public static IntervalEstimation<Double> eqnIps(
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
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				new InternalParetPostprocessProvider_sampledHist(slices, similarity, sHist, numOfConsideredValues, paretRatio));
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
	public static IntervalEstimation<Double> eqdCIps(
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
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				new InternalParetPostprocessProvider_sampledHist(slices, similarity, sHist, numOfConsideredValues, paretRatio));
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
	public static IntervalEstimation<Double> eqnCIps(
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
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				new InternalParetPostprocessProvider_sampledHist(slices, similarity, sHist, numOfConsideredValues, paretRatio));
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
	public static IntervalEstimation<Double> eqdGpIps(
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
				new InternalParetPostprocessProvider_sampledHist(slices, similarity, sHist, numOfConsideredValues, paretRatio));
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
	public static IntervalEstimation<Double> eqnGpIps(
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
				new InternalParetPostprocessProvider_sampledHist(slices, similarity, sHist, numOfConsideredValues, paretRatio));
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
	public static IntervalEstimation<Double> eqdCGpIps(
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
				new InternalParetPostprocessProvider_sampledHist(slices, similarity, sHist, numOfConsideredValues, paretRatio));
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
	public static IntervalEstimation<Double> eqnCGpIps(
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
				new InternalParetPostprocessProvider_sampledHist(slices, similarity, sHist, numOfConsideredValues, paretRatio));
		return est;
	}

	@Override
	public String paramStr() {
		return new StringBuilder()
				.append("vls=")
				.append(this.numOfConsideredValues)
				.append(".pr=")
				.append(this.paretRatio)
				.toString();
	}

	@Override
	public Map<String, String> params() {
		return Map.of("vls", Integer.toString(numOfConsideredValues),
				"pr", Double.toString(this.paretRatio));
	}
}
