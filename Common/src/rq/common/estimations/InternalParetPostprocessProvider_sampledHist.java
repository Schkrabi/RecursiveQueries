package rq.common.estimations;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import rq.common.estimations.IntervalEstimation.IntervalPostprocessProvider;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SampledHistogram;
import rq.common.util.Pair;

public class InternalParetPostprocessProvider_sampledHist implements IntervalPostprocessProvider {

	public final int slices;
	public final BiFunction<Object, Object, Double> similarity;
	private SampledHistogram hist;
	public final int numOfConsideredValues;
	public final double paretRatio;
	
	public InternalParetPostprocessProvider_sampledHist(
			int slices, 
			BiFunction<Object, Object, Double> similarity, 
			SampledHistogram hist,
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
	public RankHistogram postprocess(RankHistogram hist, int count, double representative) {
		var est = new ParRepConstSampl(
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
	public static IntervalEstimation eqdIps(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquidistantHistogram hist,
			int numOfConsideredValues,
			double paretRatio,
			SampledHistogram sHist) {
		var est = new IntervalEstimation(
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
	public static IntervalEstimation eqnIps(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquinominalHistogram hist,
			int numOfConsideredValues,
			double paretRatio,
			SampledHistogram sHist) {
		var est = new IntervalEstimation(
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
	public static IntervalEstimation eqdCIps(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquidistantHistogram hist,
			int numOfConsideredValues,
			double paretRatio,
			SampledHistogram sHist) {
		var est = new IntervalEstimation(
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
	public static IntervalEstimation eqnCIps(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquinominalHistogram hist,
			int numOfConsideredValues,
			double paretRatio,
			SampledHistogram sHist) {
		var est = new IntervalEstimation(
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
	public static IntervalEstimation eqdGpIps(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquidistantHistogram hist,
			int numOfConsideredValues,
			double paretRatio,
			SampledHistogram sHist) {
		var est = new IntervalEstimation(
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
	public static IntervalEstimation eqnGpIps(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquinominalHistogram hist,
			int numOfConsideredValues,
			double paretRatio,
			SampledHistogram sHist) {
		var est = new IntervalEstimation(
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
	public static IntervalEstimation eqdCGpIps(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquidistantHistogram hist,
			int numOfConsideredValues,
			double paretRatio,
			SampledHistogram sHist) {
		var est = new IntervalEstimation(
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
	public static IntervalEstimation eqnCGpIps(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquinominalHistogram hist,
			int numOfConsideredValues,
			double paretRatio,
			SampledHistogram sHist) {
		var est = new IntervalEstimation(
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
