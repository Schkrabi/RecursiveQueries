package rq.common.estimations;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import rq.common.operators.Selection;
import rq.common.statistic.RankHistogram;

public class Numerical_domainPruning extends Numerical {

	protected double similarityDistance;
	
	public Numerical_domainPruning(Selection selection, int resultSlices, double domainSampleSize) {
		super(selection, resultSlices, domainSampleSize);
		this.similarityDistance = this.similarityDistance();
	}
	
	/**
	 * Returns distance in which similarity is > 0
	 * @return double distance
	 */
	protected double similarityDistance() {
		double current = this.domainMin;
		
		while(this.similarity.apply(this.domainMin, current) > 0d) {
			current += this.domainSampleSize;
		}
		
		return Math.abs(current - this.domainMin);
	}

	/**
	 * Gets all non zero similar samples for the value
	 * @param value
	 * @return set of samples
	 */
	public Set<Double> nonZeroSimilarSamples(double value){
		Set<Double> rslt = new HashSet<Double>();
		
		for(double val = Math.max((value - this.similarityDistance) + this.domainSampleSize, this.domainMin);
				val < Math.min(value + this.similarityDistance, this.domainMax);
				val += this.domainSampleSize) {
			rslt.add(val);
		}
		
		return rslt;
	}
	
	/**
	 * Gets all non-zero ranks that the record with the value can fall into. 
	 * @param value
	 * @return
	 */
	public List<Double> nonZeroRanks(double value){
		return this.nonZeroSimilarSamples(value).stream()
				.map(v -> this.similarity.apply(v, value))
				.collect(Collectors.toList());
	}
	
	@Override
	protected RankHistogram estimateProbability(Set<Object> histValues) {		
		List<Double> rankList = new LinkedList<Double>();
		
		for(Object histValue : histValues) {
			long count = (long)(this.attributeHistogram.getCount((double)histValue) / this.domainSamples); 
			for(double rank : this.nonZeroRanks((double)histValue)) {
				Stream.generate(() -> rank).limit((long) count).forEach(r -> rankList.add(r));
			}
		}
		
		RankHistogram rslt = RankHistogram.build(rankList, this.resultSlices);
		
		return rslt;
	}
}
