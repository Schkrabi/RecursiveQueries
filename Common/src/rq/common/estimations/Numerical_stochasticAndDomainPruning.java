package rq.common.estimations;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import rq.common.operators.Selection;
import rq.common.statistic.RankHistogram;

public class Numerical_stochasticAndDomainPruning extends Numerical_domainPruning {

	public final int samples;
	
	public Numerical_stochasticAndDomainPruning(Selection selection, int resultSlices,
			 double domainSampleSize, int samples) {
		super(selection, resultSlices, domainSampleSize);
		this.samples = samples;
	}

	@Override
	protected RankHistogram estimateProbability(Set<Object> histValues) {		
		int localSamples = Math.min(histValues.size(), this.samples);
		
		long count = (long)(histValues.stream()
				.map(x -> (double)this.attributeHistogram.getCount((double)x))
				.reduce(0.d,  (x, y) -> x + y)
				/ localSamples)
				/ this.domainSamples;
		
		Set<Integer> sampleIndices = this.sampleIndices(localSamples);
		List<Object> histValuesList = new ArrayList<Object>(histValues);
		List<Double> rankList = new LinkedList<Double>();
		
		for(int idx : sampleIndices) {
			double histValue = (double)histValuesList.get(idx);
			for(double rank : this.nonZeroRanks((double)histValue)) {
				Stream.generate(() -> rank).limit(count).forEach(r -> rankList.add(r));
			}
		}
		
		RankHistogram rslt = RankHistogram.build(rankList, this.resultSlices);
		
		return rslt;
	}
}
