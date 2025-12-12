package rq.common.estimations;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import rq.common.estimations.IntervalEstimation.IntervalPostprocessProvider;
import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.util.Pair;

public class InternalParetPostprocessProvider_intervalHist implements IntervalPostprocessProvider {

	private final DataSlicedHistogram hist;
	public final int slices;
	public final BiFunction<Object, Object, Double> similarity;
	public final int numOfConsideredValues;
	public final double paretRatio;
	private final RepresentativeProvider representativeProvider;
	
	public InternalParetPostprocessProvider_intervalHist(
			DataSlicedHistogram hist,
			int slices,
			BiFunction<Object, Object, Double> similarity,
			int numOfConsideredValues,
			double paretRatio,
			RepresentativeProvider representativeProvider) {
		this.hist = hist;
		this.slices = slices;
		this.similarity = similarity;
		this.numOfConsideredValues = numOfConsideredValues;
		this.paretRatio = paretRatio;
		this.representativeProvider = representativeProvider;
	}

	@Override
	public String signature() {
		return "Ipi";
	}

	@Override
	public Map<String, String> params() {
		return Map.of(
				"slcs", Integer.toString(this.slices),
				"att", hist.observed.name,
				"vls", Integer.toString(this.numOfConsideredValues),
				"pr", Double.toString(this.paretRatio));
	}

	@Override
	public RankHistogram postprocess(RankHistogram hist, int count, double representative) {
		var est = new ParRepConstInt(
				this.slices, 
				this.similarity, 
				this.hist, 
				representative,
				this.numOfConsideredValues,
				this.representativeProvider);
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
	public static IntervalEstimation eqdIpi(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquidistantHistogram hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation(
				slices, 
				similarity,
				hist,
				IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER,
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				new InternalParetPostprocessProvider_intervalHist(
						hist,
						slices, 
						similarity, 
						numOfConsideredValues, 
						paretRatio,
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER));
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
	public static IntervalEstimation eqnIpi(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquinominalHistogram hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation(
				slices, 
				similarity,
				hist,
				IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER,
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				new InternalParetPostprocessProvider_intervalHist(
						hist,
						slices, 
						similarity, 
						numOfConsideredValues, 
						paretRatio,
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER));
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
	public static IntervalEstimation eqdCIpi(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquidistantHistogram hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation(
				slices, 
				similarity,
				hist,
				new GlobalCenterRepresentativeProvider(hist),
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				new InternalParetPostprocessProvider_intervalHist(
						hist,
						slices, 
						similarity, 
						numOfConsideredValues, 
						paretRatio,
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER));
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
	public static IntervalEstimation eqnCIpi(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquinominalHistogram hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation(
				slices, 
				similarity,
				hist,
				new GlobalCenterRepresentativeProvider(hist),
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				new InternalParetPostprocessProvider_intervalHist(
						hist,
						slices, 
						similarity, 
						numOfConsideredValues, 
						paretRatio,
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER));
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
	public static IntervalEstimation eqdGpIpi(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquidistantHistogram hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation(
				slices, 
				similarity,
				hist,
				IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER,
				new WeighterdParetPostprocessProvider_intervalHist(
						slices, 
						similarity, 
						numOfConsideredValues, 
						paretRatio, 
						hist,
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER),
				new InternalParetPostprocessProvider_intervalHist(
						hist,
						slices, 
						similarity, 
						numOfConsideredValues, 
						paretRatio,
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER));
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
	public static IntervalEstimation eqnGpIpi(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquinominalHistogram hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation(
				slices, 
				similarity,
				hist,
				IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER,
				new WeighterdParetPostprocessProvider_intervalHist(
						slices, 
						similarity, 
						numOfConsideredValues, 
						paretRatio, 
						hist,
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER),
				new InternalParetPostprocessProvider_intervalHist(
						hist,
						slices, 
						similarity, 
						numOfConsideredValues, 
						paretRatio,
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER));
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
	public static IntervalEstimation eqdCGpIpi(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquidistantHistogram hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation(
				slices, 
				similarity,
				hist,
				new GlobalCenterRepresentativeProvider(hist),
				new WeighterdParetPostprocessProvider_intervalHist(
						slices, 
						similarity, 
						numOfConsideredValues, 
						paretRatio, 
						hist,
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER),
				new InternalParetPostprocessProvider_intervalHist(
						hist,
						slices, 
						similarity, 
						numOfConsideredValues, 
						paretRatio,
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER));
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
	public static IntervalEstimation eqnCGpIpi(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquinominalHistogram hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation(
				slices, 
				similarity,
				hist,
				new GlobalCenterRepresentativeProvider(hist),
				new WeighterdParetPostprocessProvider_intervalHist(
						slices, 
						similarity, 
						numOfConsideredValues, 
						paretRatio, 
						hist,
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER),
				new InternalParetPostprocessProvider_intervalHist(
						hist,
						slices, 
						similarity, 
						numOfConsideredValues, 
						paretRatio,
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER));
		return est;
	}
}
