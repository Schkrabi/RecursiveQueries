package rq.common.estimations;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import rq.common.operators.Selection;
import rq.common.restrictions.Similar;
import rq.common.statistic.AttributeHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.Size;
import rq.common.statistic.Statistics;

public class Nominal extends ProbeableEstimation {

	public final int probedAttributes;
	protected final Set<Object> attributeDomain;
	
	protected final AttributeHistogram argumentAttributeHistogram;
	protected final Size argumentSize;
	
	public Nominal(
			Selection selection, 
			int resultSlices,
			int probedAttributes,
			Set<Object> attributeDomain) {
		super(selection, 
				resultSlices);
		this.probedAttributes = probedAttributes;
		this.attributeDomain = new HashSet<Object>(attributeDomain);
		
		if(!(this.condition instanceof Similar)) {
			throw new RuntimeException("Restriction condition must be Similarity");
		}
		
		Statistics stats = argument.getStatistics();
		
		Optional<AttributeHistogram> oah = stats.getAttributeHistogram(attribute); 
		if(oah.isEmpty()) {
			throw new RuntimeException("Argument must gather attribute histogram of the condition attribute.");
		}
		this.argumentAttributeHistogram = oah.get();
		
		Optional<Size> os = stats.getSize();
		if(os.isEmpty()) {
			throw new RuntimeException("Argument must gather its size.");
		}
		this.argumentSize = os.get();
	}	

	@Override
	protected RankHistogram estimateProbability(Set<Object> histValues) {		
		double size = this.argumentAttributeHistogram.getHistogram()
				.entrySet().stream()
				.filter(e -> histValues.contains(e.getKey()))
				.mapToDouble(e -> e.getValue())
				.sum();
		
		List<Double> rankList = new LinkedList<Double>();
		
		for(Object domValue : this.attributeDomain) {
			for(Object histValue : histValues) {
				long value = (long)(this.argumentAttributeHistogram.getCount(histValue) / size);
				double rank = this.similarity.apply(domValue, histValue);
				
				Stream.generate(() -> rank).limit(value).forEach(r -> rankList.add(r));
			}
		}
		RankHistogram rslt = RankHistogram.build(rankList, this.resultSlices);
		
		return rslt;	
	}

	@Override
	protected Map<Object, Integer> getHistogramData() {
		return this.argumentAttributeHistogram.getHistogram();
	}
}
