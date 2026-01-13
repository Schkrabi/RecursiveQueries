/**
 * 
 */
package rq.common.estimations;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import rq.common.operators.Selection;
import rq.common.statistic.RankHistogram;

/**
 * 
 */
public class Nominal_stochastic<T> extends Nominal<T> {

	public final int samples;
	
	/**
	 * @param selection
	 * @param argument
	 * @param resultSlices
	 * @param probedAttributes
	 * @param attributeDomain
	 * @param similarity
	 */
	public Nominal_stochastic(Selection selection, int resultSlices,
			int probedAttributes, Set<T> attributeDomain, int restSamples) {
		super(selection, resultSlices, probedAttributes, attributeDomain);
		this.samples = restSamples;
	}

	@Override
	protected RankHistogram estimateProbability(Set<T> histValues) {
		Set<Integer> sampleIndices = this.sampleIndices(Math.min(histValues.size(), this.samples));
		
		double size = (double)this.argumentAttributeHistogram.getHistogram().entrySet().stream()
				.filter(e -> histValues.contains(e.getKey()))
				.map(e -> e.getValue())
				.reduce(0, (x, y) -> x + y);
		double added = size / (double)sampleIndices.size();
		
		List<T> histValuesList = histValues.stream().collect(Collectors.toList());
		
		List<Double> rankList = new LinkedList<Double>();
		
		for(int i : sampleIndices) {
			T histValue = histValuesList.get(i);
			for(T domValue : this.attributeDomain) {
				Double rank = this.similarity.apply(domValue, histValue);
				Stream.generate(() -> rank).limit((long) added).forEach(r -> rankList.add(r));
			}
		}
		
		RankHistogram rslt = RankHistogram.build(rankList, this.resultSlices);
		
		return rslt;
	}
	
	public static <U> RankHistogram estimateStatic(Selection selection, int resultSlices,
		int probedAttributes, Set<U> attributeDomain, int restSamples){
		var me = new Nominal_stochastic<U>(selection, resultSlices, probedAttributes, attributeDomain, restSamples);
		var rslt = me.estimate();
		return rslt;
	}
}
