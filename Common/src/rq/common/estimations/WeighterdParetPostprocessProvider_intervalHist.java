package rq.common.estimations;

import java.util.List;
import java.util.Map;

import rq.common.estimations.IntervalEstimation.GlobalPostprocessProvider;
import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.util.Pair;

public class WeighterdParetPostprocessProvider_intervalHist implements GlobalPostprocessProvider {

	public final int slices;
	public final rq.common.similarities.ISimilarity<Double> similarity;
	public final int numOfConsideredValues;
	private final DataSlicedHistogram<Double> hist;
	public final double paretRatio;
	private final RepresentativeProvider<Double> representativeProvider;
	
	public WeighterdParetPostprocessProvider_intervalHist(
			int slices,
			rq.common.similarities.ISimilarity<Double> similarity,
			int numOfConsideredValues,
			double paretRatio,
			DataSlicedHistogram<Double> hist,
			RepresentativeProvider<Double> representativeProvider) {
		this.slices = slices;
		this.similarity = similarity;
		this.numOfConsideredValues = numOfConsideredValues;
		this.hist = hist;
		this.representativeProvider = representativeProvider;
		
		if(paretRatio < 0 || paretRatio > 1.0d) {
			throw new RuntimeException("Paret ratio must be between 0 and 1, got " + Double.toString(paretRatio));
		}
		this.paretRatio = paretRatio;
	}

	@Override
	public String signature() {
		return "Gpi";
	}

	@Override
	public Map<String, String> params() {
		return Map.of("vls", Integer.toString(numOfConsideredValues),
				"pr", Double.toString(this.paretRatio));
	}

	@Override
	public RankHistogram postprocess(RankHistogram hist) {
		var est = new ParetWeightedEstimation_intervalHist(
				this.slices,
				this.similarity,
				this.numOfConsideredValues,
				this.hist,
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
	public static IntervalEstimation<Double> eqdGpi(
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
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
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
	public static IntervalEstimation<Double> eqnGpi(
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
				new WeighterdParetPostprocessProvider_intervalHist(
						slices, 
						similarity, 
						numOfConsideredValues, 
						paretRatio, 
						hist,
						IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER),
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
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
	public static IntervalEstimation<Double> eqdCGpi(
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
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
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
	public static IntervalEstimation<Double> eqnCGpi(
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
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
		return est;
	}

}
