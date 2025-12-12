package rq.common.estimations;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import rq.common.estimations.IntervalEstimation.GlobalPostprocessProvider;
import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.util.Pair;

public class WeighterdParetPostprocessProvider_intervalHist implements GlobalPostprocessProvider {

	public final int slices;
	public final BiFunction<Object, Object, Double> similarity;
	public final int numOfConsideredValues;
	private final DataSlicedHistogram hist;
	public final double paretRatio;
	private final RepresentativeProvider representativeProvider;
	
	public WeighterdParetPostprocessProvider_intervalHist(
			int slices,
			BiFunction<Object, Object, Double> similarity,
			int numOfConsideredValues,
			double paretRatio,
			DataSlicedHistogram hist,
			RepresentativeProvider representativeProvider) {
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
	public static IntervalEstimation eqdGpi(
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
	public static IntervalEstimation eqnGpi(
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
	public static IntervalEstimation eqdCGpi(
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
	public static IntervalEstimation eqnCGpi(
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
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
		return est;
	}

}
