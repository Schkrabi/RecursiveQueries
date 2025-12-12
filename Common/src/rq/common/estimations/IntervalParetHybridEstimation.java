package rq.common.estimations;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.MostCommonValues;
import rq.common.statistic.RankHistogram;
import rq.common.util.Pair;

public abstract class IntervalParetHybridEstimation implements IEstimation {

	protected final DataSlicedHistogram hist;
	private final Collection<Pair<Double, Integer>> mostCommon;
	public final int slices;
	public final BiFunction<Object, Object, Double> similarity;
	public final double c;
	
	/**
	 * 
	 * @param slices Number of unifor slices in final hist
	 * @param hist value histogram (eqd or eqn)
	 * @param mostCommon collection of most common values with counts
	 * @param similarity 
	 * @param c constant to evaluate the paret precise estimation against
	 */
	public IntervalParetHybridEstimation(
			int slices,
			DataSlicedHistogram hist,
			Collection<Pair<Double, Integer>> mostCommon,
			BiFunction<Object, Object, Double> similarity,
			double c) {
		this.slices = slices;
		this.hist = hist;
		this.mostCommon = mostCommon;
		this.similarity = similarity;
		this.c = c;
	}
	
	public abstract IEstimation subestimation(DataSlicedHistogram sHist);

	@Override
	public String signature() {
		return new StringBuilder()
				.append("H") // Hybrid
				.append(this.hist instanceof EquinominalHistogram ? "eqn" : 
					    this.hist instanceof EquidistantHistogram ? "eqd" : "gen")
				.append("ppc")
				.toString();
	}

	@Override
	public RankHistogram estimate() {
		var sHist = this.hist.removeValueCount(this.mostCommon); //Substracted histogram
		var intv = this.subestimation(sHist);
		
		var ppc = new ParPrecConst(
				this.hist.observed, 
				this.slices, 
				this.c, 
				this.similarity, 
				this.mostCommon);
		
		var mcc = this.mostCommon.stream() //Most common count
				.mapToDouble(p -> (double)p.second).sum();
		
		var ratio = mcc/this.hist.totalSize();
		var intv_h = intv.estimate();
		var ppc_h = ppc.estimate();
		
		var est = RankHistogram.weightedAvg(List.of(
				Pair.of(intv_h, 1.0 - ratio),
				Pair.of(ppc_h, ratio)));
		
		return est;
	}

	@Override
	public int getSlices() {
		return this.slices;
	}

	@Override
	public Map<String, String> _params() {
		return Map.of(
				"vls", Integer.toString(this.mostCommon.size()),
				"slcs", Integer.toString(this.slices),
				"n", Integer.toString(this.hist.n),
				"att", this.hist.observed.name,
				"c", Double.toString(this.c));
	}
	
	public static IEstimation knownConstant(
			int slices,
			DataSlicedHistogram hist,
			MostCommonValues mcv,
			BiFunction<Object, Object, Double> similarity,
			double c) {
		return new IntervalParetHybridEstimation(
				slices,
				hist,
				mcv.mostCommon(20), //Magic constant
				similarity,
				c) {

					@Override
					public IEstimation subestimation(DataSlicedHistogram sHist) {
						return IntervalEstimation.fromHist(this.slices, this.similarity, sHist);
					}
		};
	}
	
	public static IEstimation unknownConstant(
			int slices,
			DataSlicedHistogram hist,
			MostCommonValues mcv,
			BiFunction<Object, Object, Double> similarity) {
		return new IntervalParetHybridEstimation(
				slices,
				hist,
				mcv.mostCommon(20), //Magic constant
				similarity,
				mcv.centerOfGravity()) {

					@Override
					public String signature() {
						return new StringBuilder()
								.append("H") // Hybrid
								.append(this.hist instanceof EquinominalHistogram ? "eqnK" : 
									    this.hist instanceof EquidistantHistogram ? "eqdK" : "genK")
								.append("ppc")
								.toString();
					}
			
					@Override
					public IEstimation subestimation(DataSlicedHistogram sHist) {
						return ConstantRepresentativeProvider.fromHist(this.slices, this.similarity, sHist, this.c);
					}
		};
	}

}
