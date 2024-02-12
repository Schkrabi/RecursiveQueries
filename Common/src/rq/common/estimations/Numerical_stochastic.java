package rq.common.estimations;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import rq.common.operators.Selection;
import rq.common.statistic.RankHistogram;

public class Numerical_stochastic extends Numerical {

	public final int samples;
	
	public Numerical_stochastic(Selection selection, int resultSlices,
			 double domainSampleSize, int samples) {
		super(selection, resultSlices, domainSampleSize);
		this.samples = samples;
	}

	
	@Override
	protected RankHistogram estimateProbability(Set<Object> histValues) {		
		int localSamples = Math.min(histValues.size(), this.samples);
		
		//Need to count sample size per number of samples in histogram and per number of samples of the domain
		long count = (long)((histValues.stream()
				.mapToDouble(x -> (double)this.attributeHistogram.getCount((double)x))
				.sum()
				/ localSamples))
				/ this.domainSamples;
		
		Set<Integer> sampleIndices = this.sampleIndices(localSamples);
		List<Object> histValuesList = new ArrayList<Object>(histValues);
		List<Double> rankList = new LinkedList<Double>();
		
		for(int idx : sampleIndices) {
			double histValue = (double)histValuesList.get(idx);
			//Full walk through domain
			for(	double domValue = this.domainMin; 
					domValue < this.domainMax; 
					domValue += this.domainSampleSize) {
				double rank = this.similarity.apply(histValue, domValue);
				Stream.generate(() -> rank).limit((long) count).forEach(r -> rankList.add(r));
			}
		}
		
		RankHistogram rslt = RankHistogram.build(rankList, this.resultSlices);
		
		return rslt;
	}
}
