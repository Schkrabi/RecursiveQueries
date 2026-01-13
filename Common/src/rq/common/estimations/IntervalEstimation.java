package rq.common.estimations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.DataSlicedHistogram.Interval;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.util.Pair;
import rq.common.similarities.ISimilarity;

/** Abstract class for the interval based restriction estimation*/
public class IntervalEstimation<T extends Number> implements IEstimation {
	
	private final RepresentativeProvider<T> representativeProvider;
	private final GlobalPostprocessProvider globalPostprocessProvider;
	private final IntervalPostprocessProvider<T> intervalPostrprocessProvider;
	
	/** number of result slices*/
	public final int slices;
	/** similarity function used*/
	protected final ISimilarity<Double> similarity;
	/** interval histogram used for estimation*/
	protected final DataSlicedHistogram<T> dataIntervals;
	
	protected IntervalEstimation(
			int slices, 
			ISimilarity<Double> similarity,
			DataSlicedHistogram<T> dataIntervals,
			RepresentativeProvider<T> representativeProvider,
			GlobalPostprocessProvider globalPostprocessProvider,
			IntervalPostprocessProvider<T> intervalPostprocessProvider) {
		this.slices = slices;
		this.similarity = similarity;
		this.dataIntervals = dataIntervals;
		this.representativeProvider = representativeProvider;
		this.globalPostprocessProvider = globalPostprocessProvider;
		this.intervalPostrprocessProvider = intervalPostprocessProvider;
	}
	
	@Override
	public RankHistogram estimate() {
		var histograms = new ArrayList<Pair<RankHistogram, Double>>();
		
		for(var dataInterval : this.dataIntervals.intervals()) {
			var count = this.dataIntervals.get(dataInterval);
			var representative = this.representative(dataInterval);
			var histogram = new RankHistogram(this.slices);
			histogram.addRanks(this.ranksForInterval(dataInterval, count, representative));
			histogram = this.intervalPostprocess(histogram, count, representative);
			var share = (dataInterval.to - dataInterval.from) / this.totalRange();
			histograms.add(Pair.of(histogram, share));
		}
		
		var rslt = RankHistogram.weightedAvg(histograms);
		rslt = this.postprocess(rslt);
		return rslt;
	}
	
	/** Returns list of computed ranks */
	protected List<Double> ranksForInterval(Interval dataInterval, int count, T value) {
		List<Double> result = new ArrayList<Double>();

		double step = (dataInterval.to - dataInterval.from) / count;
		// x is an iterating variable used to traverse the value interval
		double x;
		if (value.doubleValue() <= dataInterval.from) {
			x = dataInterval.from;
		} else {
			// Reverses the order in which samples of the value intervals are taken
			x = dataInterval.to;
			if (!dataInterval.closedTo)
				x -= step;
			step = -step;
		}
		
		//Iterate over number of samples from the interval
		for (int i = 0; i < count; i++) {
			double rank = this.similarity.apply(x, value.doubleValue());
			if (rank == 0)
				break;
			result.add(rank);
			x += step;
		}

		return result;
	}

	/** string representing the estimation technique*/
	public String signature() {
		return new StringBuilder()
				.append(this.dataIntervals.signature())
				.append(this.representativeProvider.signature())
				.append(this.globalPostprocessProvider.signature())
				.append(this.intervalPostrprocessProvider.signature())
				.toString();
	}
	
	@Override
	public Map<String, String> _params() {
		var repP = this.representativeProvider.params();
		var gppP = this.globalPostprocessProvider.params();
		var ippP = this.intervalPostrprocessProvider.params();
		var me = new HashMap<>(Map.of("slcs", Integer.toString(slices),
				"att", this.dataIntervals.observed.name,
				"int", Integer.toString(this.dataIntervals.n)));
		me.putAll(repP);
		me.putAll(gppP);
		me.putAll(ippP);
		return me;
	}
	
	public static interface RepresentativeProvider<T> extends SignatureProvider, IParametrized {
		public T representative (Interval interval);
	}
	
	private static class DefaultRepresentativeProvider
		implements RepresentativeProvider<Double> {

		@Override
		public Double representative(Interval interval) {
			var min = Math.min(interval.from, interval.to);
			var max = Math.max(interval.from, interval.to);
			
			//Abs probably not necessary
			var d = Math.abs(max - min);
			return min + (d/2);
		}

		@Override
		public String signature() {
			return "";
		}

		@Override
		public Map<String, String> params() {
			return Map.of();
		}
	}
	public static final RepresentativeProvider<Double> DEFAULT_REPRESENTATIVE_PROVIDER
		 = new DefaultRepresentativeProvider();
	
	/** returns a representative of the data interval */
	private T representative(Interval interval) {
		return this.representativeProvider.representative(interval);
	}
	
	public static interface GlobalPostprocessProvider extends SignatureProvider, IParametrized {
		public RankHistogram postprocess(RankHistogram hist);
	}
	
	private static class DefaultGlobalPostprocessProvider implements GlobalPostprocessProvider {

		@Override
		public String signature() {
			return "";
		}

		@Override
		public RankHistogram postprocess(RankHistogram hist) {
			return hist;
		}

		@Override
		public Map<String, String> params() {
			return Map.of();
		}
	}
	
	public static final GlobalPostprocessProvider DEFAULT_GLOBAL_POSTPROCESS_PROVIDER = 
			new DefaultGlobalPostprocessProvider();
	
	/** postprocess the resulting estimated histogram*/
	protected RankHistogram postprocess(RankHistogram hist) {
		return this.globalPostprocessProvider.postprocess(hist);
	}
	
	public static interface IntervalPostprocessProvider<T extends Number> extends SignatureProvider, IParametrized {
		public RankHistogram postprocess(RankHistogram hist, int count, T representative);
	}
	
	public static class DefaultIntervalPostprocessProvider<T extends Number> implements IntervalPostprocessProvider<T> {

		@Override
		public String signature() {
			return "";
		}

		@Override
		public RankHistogram postprocess(RankHistogram hist, int count, T representative) {
			return hist;
		}


		@Override
		public Map<String, String> params() {
			return Map.of();
		}	
	}
	
	public static final IntervalPostprocessProvider<Double> DEFAULT_INTERVAL_POSTPROCESS_PROVIDER = 
			new DefaultIntervalPostprocessProvider<>();
	
	
	/** postprocess the intermediate interval histogram*/
	protected RankHistogram intervalPostprocess(RankHistogram hist, int count, T representative) {
		return this.intervalPostrprocessProvider.postprocess(hist, count, representative);
	}
	
	private Double _min = null;
	/** effective domain minimum */
	protected double getMin() {
		if(_min == null) {
			_min = this.dataIntervals.intervals().stream().mapToDouble(i -> i.from).min().getAsDouble();
		}
		return _min;
	}
	
	private Double _max = null;
	/** effective domain maximum*/
	protected double getMax() {
		if(_max == null) {
			_max = this.dataIntervals.intervals().stream().mapToDouble(i -> i.to).max().getAsDouble();
		}
		return _max;
	}
	
	private Double _totalRange = null;
	/* total range (size) of the effective domain*/
	protected double totalRange() {
		if(_totalRange == null) {
			_totalRange = this.getMax() - this.getMin();
		}
		return _totalRange;
	}
	
	public static final SignatureProvider EQD_SIGNATURE_PROVIDER = new SignatureProvider() {

		@Override
		public String signature() {
			return "eqd";
		}
		
	};

	public static IntervalEstimation<Double> eqd(
			int slices, 
			ISimilarity<Double> similarity,
			EquidistantHistogram<Double> hist) {
		var est = new IntervalEstimation<Double>(
				slices, 
				similarity,
				hist,
				DEFAULT_REPRESENTATIVE_PROVIDER,
				DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
		return est;
	}
	
	public static final SignatureProvider EQN_SIGNATURE_PROVIDER = new SignatureProvider() {

		@Override
		public String signature() {
			return "eqn";
		}
		
	};
	
	public static IntervalEstimation<Double> eqn(
			int slices, 
			ISimilarity<Double> similarity,
			EquinominalHistogram<Double> hist) {
		var est = new IntervalEstimation<>(
				slices, 
				similarity,
				hist,
				DEFAULT_REPRESENTATIVE_PROVIDER,
				DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
		return est;
	}

	@Override
	public int getSlices() {
		return this.slices;
	}
	
	public static IntervalEstimation<Double> fromHist(
			int slices,
			ISimilarity<Double> similarity,
			DataSlicedHistogram<Double> hist
			) {
		if(hist instanceof EquidistantHistogram<Double> eqd) {
			return eqd(slices, similarity, eqd);
		}
		else if(hist instanceof EquinominalHistogram<Double> eqn) {
			return eqn(slices, similarity, eqn);
		}
		return new IntervalEstimation<>(
				slices,
				similarity,
				hist,
				DEFAULT_REPRESENTATIVE_PROVIDER,
				DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
	}
}
