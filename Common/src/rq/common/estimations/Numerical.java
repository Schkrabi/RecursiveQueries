package rq.common.estimations;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import rq.common.operators.Selection;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SampledHistogram;

public class Numerical extends ProbeableEstimation<Double> {

	public final double domainMin;
	public final double domainMax;
	public final double domainSize;
	public final int domainSamples;
	public final double domainSampleSize;
	
	protected final SampledHistogram<Double> attributeHistogram;
	
	public Numerical(
			Selection selection,  
			int resultSlices,
			double domainSampleSize,
			SampledHistogram<Double> h) {
		super(selection, resultSlices);
		
		var stats = this.argument.getStatistics();
		this.domainSampleSize = domainSampleSize;
		
		SampledHistogram<Double> hist = null;
		if(h == null) {
			hist = stats
				.<Double>getSampledHistogram(this.attribute, domainSampleSize).get();
		}
		else {
			hist = h;
		}

		this.attributeHistogram = hist;
		this.domainMin = hist.min();
		this.domainMax = hist.max();
		this.domainSize = this.domainMax - this.domainMin;
		
		
		this.domainSamples = (int)(this.domainSize / this.domainSampleSize);
	}
	
	/**
	 * Gets the sample appropriate for given value
	 * @param value
	 * @return
	 */
	protected double valueSample(double value) {
		long mult = Math.round(value / this.domainSampleSize);
		
		return mult * this.domainSampleSize;
	}
	
	@Override
	protected RankHistogram estimateProbability(Set<Double> histValues) {
		List<Double> rankList = new LinkedList<Double>();
		
		for(Double histValue : histValues) {
			double count = (double)this.attributeHistogram.getCount((double)histValue) / this.domainSamples; 
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

	@Override
	protected Map<Double, Integer> getHistogramData() {
		return new HashMap<Double, Integer>(this.attributeHistogram.getHistogram());
	}

	
	public static RankHistogram estimateStatic(Selection selection,  
			int resultSlices,
			double domainSampleSize,
			int probes,
			SampledHistogram<Double> hist) {
		var me = new Numerical(selection, resultSlices, domainSampleSize, hist);
		me.setProbes(probes);
		var est = me.estimate();
		return est;
	}
}
