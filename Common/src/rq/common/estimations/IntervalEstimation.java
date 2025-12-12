package rq.common.estimations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.DataSlicedHistogram.Interval;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.util.Pair;

/** Abstract class for the interval based restriction estimation*/
public class IntervalEstimation implements IEstimation {
	
	private final RepresentativeProvider representativeProvider;
	private final GlobalPostprocessProvider globalPostprocessProvider;
	private final IntervalPostprocessProvider intervalPostrprocessProvider;
	
	/** number of result slices*/
	public final int slices;
	/** similarity function used*/
	protected final BiFunction<Object, Object, Double> similarity;
	/** interval histogram used for estimation*/
	protected final DataSlicedHistogram dataIntervals;
	
	protected IntervalEstimation(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			DataSlicedHistogram dataIntervals,
			RepresentativeProvider representativeProvider,
			GlobalPostprocessProvider globalPostprocessProvider,
			IntervalPostprocessProvider intervalPostprocessProvider) {
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
	protected List<Double> ranksForInterval(Interval dataInterval, int count, Double value) {
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
	
	public static interface RepresentativeProvider extends SignatureProvider, IParametrized {
		public double representative (Interval interval);
	}
	
	private static class DefaultRepresentativeProvider 
		implements RepresentativeProvider {

		@Override
		public double representative(Interval interval) {
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
	public static final RepresentativeProvider DEFAULT_REPRESENTATIVE_PROVIDER
		 = new DefaultRepresentativeProvider();
	
	/** returns a representative of the data interval */
	private double representative(Interval interval) {
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
	
	public static interface IntervalPostprocessProvider extends SignatureProvider, IParametrized {
		public RankHistogram postprocess(RankHistogram hist, int count, double representative);
	}
	
	private static class DefaultIntervalPostprocessProvider implements IntervalPostprocessProvider {

		@Override
		public String signature() {
			return "";
		}

		@Override
		public RankHistogram postprocess(RankHistogram hist, int count, double representative) {
			return hist;
		}


		@Override
		public Map<String, String> params() {
			return Map.of();
		}	
	}
	
	public static final IntervalPostprocessProvider DEFAULT_INTERVAL_POSTPROCESS_PROVIDER = 
			new DefaultIntervalPostprocessProvider();
	
	
	/** postprocess the intermediate interval histogram*/
	protected RankHistogram intervalPostprocess(RankHistogram hist, int count, double representative) {
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

	public static IntervalEstimation eqd(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquidistantHistogram hist) {
		var est = new IntervalEstimation(
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
	
	public static IntervalEstimation eqn(
			int slices, 
			BiFunction<Object, Object, Double> similarity,
			EquinominalHistogram hist) {
		var est = new IntervalEstimation(
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
	
	public static IntervalEstimation fromHist(
			int slices,
			BiFunction<Object, Object, Double> similarity,
			DataSlicedHistogram hist
			) {
		if(hist instanceof EquidistantHistogram eqd) {
			return eqd(slices, similarity, eqd);
		}
		else if(hist instanceof EquinominalHistogram eqn) {
			return eqn(slices, similarity, eqn);
		}
		return new IntervalEstimation(
				slices,
				similarity,
				hist,
				DEFAULT_REPRESENTATIVE_PROVIDER,
				DEFAULT_GLOBAL_POSTPROCESS_PROVIDER,
				DEFAULT_INTERVAL_POSTPROCESS_PROVIDER);
	}
}
