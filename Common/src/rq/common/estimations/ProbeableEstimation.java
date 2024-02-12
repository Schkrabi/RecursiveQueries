package rq.common.estimations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import rq.common.interfaces.Table;
import rq.common.onOperators.Constant;
import rq.common.operators.Selection;
import rq.common.restrictions.Similar;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SlicedStatistic.RankInterval;

public abstract class ProbeableEstimation extends AbstractSelectionEstimation {

	protected int probes;
	
	protected final Random rand = new Random();
	
	public ProbeableEstimation(Selection selection, int resultSlices) {
		super(selection, resultSlices);
		this.probes = 0;
	}
	
	public void setProbes(int probes) {
		this.probes = probes;
	}
	
	protected Set<Integer> sampleIndices(int samples) {
		Set<Integer> rslt = new HashSet<Integer>();
		
		while(rslt.size() < samples) {
			rslt.add(rand.nextInt(samples));
		}
		
		return rslt;
	}
	
	/**
	 * Pair of most frequent and rest values
	 */
	protected static class SplitedValues{
		public final List<Object> mostFrequent;
		public final List<Object> rest;
		
		public SplitedValues(List<Object> mostFrequent, List<Object> rest) {
			this.mostFrequent = mostFrequent;
			this.rest = rest;
		}
	}
	
	
	/**
	 * Splits values to most frequent and rest
	 * @param sortedValues list of values ordered by frequency
	 * @return
	 */
	protected SplitedValues splitValues(Map<Object, Integer> histogram, int probedValues){
		List<Object> sorted = histogram.entrySet().stream()
				.sorted((e1, e2) -> Integer.compare(e1.getValue(), e2.getValue()))
				.map(e -> e.getKey())
				.collect(Collectors.toList());
		List<Object> mostFrequent = new ArrayList<Object>(probedValues);
		List<Object> rest = new ArrayList<Object>(histogram.size() - probedValues);			
		
		int i = 0;
		for(Object o : sorted) {
			if(i < probedValues) {
				mostFrequent.add(o);
			}
			else {
				rest.add(o);
			}
			i++;
		}
		
		return new SplitedValues(mostFrequent, rest);
	}
	
	
	/**
	 * Probes a single attribute value
	 * @param value
	 * @return
	 */
	protected RankHistogram probeAttributeValue(Object value, Set<RankInterval> slices) {
		Selection selection = new Selection(
				this.argument,
				new Similar(this.attribute, new Constant<Object>(value), this.similarity));
		
		Table rslt = selection.eval();
		rslt.getStatistics().addRankHistogram(slices);
		rslt.getStatistics().gather();
		
		return rslt.getStatistics().getRankHistogram(slices).get();
	}
	
	protected abstract RankHistogram estimateProbability(Set<Object> histValues);
	protected abstract Map<Object, Integer> getHistogramData();
	
	/**
	 * Estimates with probes
	 * @param probes
	 * @return estimate
	 */
	public RankHistogram estimate() {
		Map<Object, Integer> atributeHistogram = this.getHistogramData();
		
		SplitedValues split = 
				this.splitValues(
						new HashMap<Object, Integer>(atributeHistogram), 
						this.probes);
		
		double tableSize = (double)atributeHistogram.entrySet()
				.stream()
				.map(e -> e.getValue())
				.reduce(0, (x, y) -> x + y);
		
		RankHistogram restEstimate = 
				this.estimateProbability(
						split.rest.stream()
						.map(x -> x)
						.collect(Collectors.toSet()));
		
		RankHistogram rslt = new RankHistogram(restEstimate.getSlices());
		
		for(Object value : split.mostFrequent) {
			RankHistogram probe = this.probeAttributeValue(value, rslt.getSlices());
			Double ratio = (double)atributeHistogram.get(value) / tableSize; 
			probe = RankHistogram.mult(probe, ratio);
			
			rslt = RankHistogram.add(rslt, probe);
		}
		
		rslt = RankHistogram.add(rslt, restEstimate);
		
		return rslt;
	}
}
