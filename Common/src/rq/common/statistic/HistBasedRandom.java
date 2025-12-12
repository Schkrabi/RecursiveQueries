package rq.common.statistic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import rq.common.estimations.IntervalEstimation.RepresentativeProvider;
import rq.common.statistic.DataSlicedHistogram.Interval;
import rq.common.util.Pair;

public class HistBasedRandom {
	private final List<Pair<Double, Interval>> distroMap;
	private final Random random;
	private final RepresentativeProvider representativeProvider;
	
	private HistBasedRandom(
			List<Pair<Double, Interval>> distroMap, 
			Random random,
			RepresentativeProvider representativeProvider) {
		this.distroMap = distroMap;
		this.random = random;
		this.representativeProvider = representativeProvider;
	}
	
	public double nextDouble() {
		// 0.0 = 3; 0.25 = 3; 0.5 = 2; 75.0 = 0
		// 0.33
		var ticket = this.random.nextDouble();
		for(var e : this.distroMap) {
			if(ticket > e.first) {
				return this.representativeProvider.representative(e.second);
			}
		}
		throw new RuntimeException("Double random generation error");
	}
	
	public DoubleStream doubles() {
		return DoubleStream.generate(() -> this.nextDouble());
	}
	
	public static HistBasedRandom fromSampledHist(
			SampledHistogram hist, 
			Random rand,
			RepresentativeProvider reprsentativeProvider) {
		//sample size : 50, count = 100
		//0 = 10; 50 = 30; 100 = 60
		//0.0 : (0,50]; 0.1 : (50,100]; 0.4 : (100,150]
		
		var cnt = (double)hist.valuesCount();
		List<Pair<Double, Interval>> h = new ArrayList<>();
		var agg = 0.0;
		for(var e : hist.getHistogram().entrySet()
				.stream().sorted((e1, e2) -> Double.compare(e1.getKey(), e2.getKey()))
				.collect(Collectors.toList())) {
			h.add(Pair.of(agg, new Interval(e.getKey(), e.getKey() + hist.sampleSize, false, true)));
			agg += (e.getValue().doubleValue() / cnt);
		}
		h = h.stream().sorted((p1, p2) -> -Double.compare(p1.first, p2.first)).collect(Collectors.toList());
		return new HistBasedRandom(h, rand, reprsentativeProvider);
	}
	
	public static HistBasedRandom fromSampledHist(
			SampledHistogram hist,
			Random rand) {
		return fromSampledHist(hist, rand, new RandRepresentativeProvider(rand));
	}
	
	public static HistBasedRandom fromSampledHist(
			SampledHistogram hist,
			long seed) {
		var rand = new Random(seed);
		return fromSampledHist(hist, rand);
	}
	
	public static HistBasedRandom fromDataSlicedHist(
			DataSlicedHistogram hist,
			Random rand,
			RepresentativeProvider representativeProvider) {
		var cnt = hist.totalSize();
		List<Pair<Double, Interval>> h = new ArrayList<>();
		var agg = 0.0d;
		for(var e : hist.data().entrySet().stream()
				.sorted((e1, e2) -> Double.compare(e1.getKey().from, e2.getKey().from))
				.collect(Collectors.toList())) {
			h.add(Pair.of(agg, e.getKey()));
			agg += (e.getValue().doubleValue() / cnt);
		}
		h = h.stream().sorted((p1, p2) -> -Double.compare(p1.first, p2.first)).collect(Collectors.toList());
		return new HistBasedRandom(h, rand, representativeProvider);
	}
	
	public static HistBasedRandom fromDataSLicedHist(
			DataSlicedHistogram hist,
			Random rand){
		return fromDataSlicedHist(hist, rand, new RandRepresentativeProvider(rand));
	}
}