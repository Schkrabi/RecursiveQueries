package rq.common.estimations;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.similarities.ISimilarity;
import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.util.Pair;

public class WeightedCompositeEstimation implements IEstimation {
	
	public final int slices;
	public final DataSlicedHistogram<Double> hist;
	public final RepresentativeProvider<Double> representativeProvider;
	public final int consideredValues;
	
	private final ISimilarity<Double> similarity;

	public WeightedCompositeEstimation(
			int slices,
			DataSlicedHistogram<Double> hist,
			RepresentativeProvider<Double> representativeProvider,
			int consideredValues,
			ISimilarity<Double> similarity) {
		this.slices = slices;
		this.hist = hist;
		this.representativeProvider = representativeProvider;
		this.consideredValues = consideredValues;
		this.similarity = similarity;
	}

	@Override
	public String signature() {
		return new StringBuilder()
				.append("wce")
				.append(this.hist.signature())
				.append(this.representativeProvider.signature())
				.toString();
	}
	
	@Override
	public Map<String, String> _params() {
		return new HashMap<>(Map.of("slcs", Integer.toString(this.slices),
				"att", this.hist.observed.name,
				"int", Double.toString(this.hist.n),
				"vls", Integer.toString(consideredValues),
				"n", Integer.toString(this.hist.n)));
	}

	@Override
	public RankHistogram estimate() {
		var size = (double)this.hist.totalSize();
		var histsToRatio = this.hist.valuesByFrequency().stream().limit(this.consideredValues)
				.map(i -> Pair.of(this.subestimate(this.representativeProvider.representative(i)), this.hist.get(i) / size))
				.collect(Collectors.toList());
		
		var rslt = RankHistogram.weightedAvg(histsToRatio);
		
		return rslt;
	}

	@Override
	public int getSlices() {
		return this.slices;
	}
	
	private RankHistogram subestimate(double representative) {
		var est = this.getSubestimation(representative);
		var hist = est.estimate();
		return hist;
	}
	
	protected IEstimation getSubestimation(double representative) {
		return new IntervalEstimation<Double>(this.slices, 
				this.similarity, 
				hist, 
				new ConstantRepresentativeProvider<>(representative), 
				IntervalEstimation.DEFAULT_GLOBAL_POSTPROCESS_PROVIDER, 
				IntervalEstimation.DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
	}
}
