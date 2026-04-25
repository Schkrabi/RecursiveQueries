package rq.common.estimations;

import java.util.List;
import java.util.Map;

import rq.common.estimations.IntervalEstimation.IntervalPostprocessProvider;
import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.util.Pair;

public class InternalParetPostprocessProvider_intervalHist implements IntervalPostprocessProvider<Double> {

	private final DataSlicedHistogram<Double> hist;
	public final int slices;
	public final rq.common.similarities.ISimilarity<Double> similarity;
	public final int numOfConsideredValues;
	public final double paretRatio;
	private final RepresentativeProvider<Double> representativeProvider;
	
	public InternalParetPostprocessProvider_intervalHist(
			DataSlicedHistogram<Double> hist,
			int slices,
			rq.common.similarities.ISimilarity<Double> similarity,
			int numOfConsideredValues,
			double paretRatio,
			RepresentativeProvider<Double> representativeProvider) {
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
	public RankHistogram postprocess(RankHistogram hist, int count, Double representative) {
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
	public static IntervalEstimation<Double> eqdIpi(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity,
			EquidistantHistogram<Double> hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation<Double>(
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
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER),
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
	public static IntervalEstimation<Double> eqnIpi(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity,
			EquinominalHistogram<Double> hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation<>(
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
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER),
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
	public static IntervalEstimation<Double> eqdCIpi(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity,
			EquidistantHistogram<Double> hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation<>(
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
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER),
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
	public static IntervalEstimation<Double> eqnCIpi(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity,
			EquinominalHistogram<Double> hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation<>(
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
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER),
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
	public static IntervalEstimation<Double> eqdGpIpi(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity,
			EquidistantHistogram<Double> hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation<>(
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
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER),
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
	public static IntervalEstimation<Double> eqnGpIpi(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity,
			EquinominalHistogram<Double> hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation<Double>(
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
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER),
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
	public static IntervalEstimation<Double> eqdCGpIpi(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity,
			EquidistantHistogram<Double> hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation<>(
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
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER),
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
	public static IntervalEstimation<Double> eqnCGpIpi(
			int slices, 
			rq.common.similarities.ISimilarity<Double> similarity,
			EquinominalHistogram<Double> hist,
			int numOfConsideredValues,
			double paretRatio) {
		var est = new IntervalEstimation<>(
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
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER),
				IntervalEstimation.WEIGHTED_AVG_AGGREGATION_PROVIDER);
		return est;
	}
}
