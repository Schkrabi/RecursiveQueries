package rq.common.statistic;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import rq.common.interfaces.Table;
import rq.common.statistic.SlicedStatistic.RankInterval;
import rq.common.table.Attribute;

public abstract class DataSlicedHistogram extends AbstractStatistic {

	public final Attribute observed;
	public final int n;
	protected Map<Interval, Integer> counts = Map.of();

	public static class Interval {
	
		public final double from;
		public final double to;
		public final boolean closedFrom;
		public final boolean closedTo;
		
		public Interval(double from, double to, boolean closedFrom, boolean closedTo) {
			this.from = from;
			this.to = to;
			this.closedFrom = closedFrom;
			this.closedTo = closedTo;
		}
		
		public boolean contains(double value) {
			return ((value > from) && (value < to))
					|| (closedFrom && value == from)
					|| (closedTo && value == to);
		}
	
		@Override
		public String toString() {
			StringBuilder buf = new StringBuilder();
			buf.append(closedFrom ? "[" : "(" );
			buf.append(from).append("; ").append(to);
			buf.append(closedTo ? "]" : ")" );
			return buf.toString();
		}
	
	}

	protected List<Double> values(Table table) {
		return table.stream()
				.map(r -> (double)r.getNoThrow(this.observed))
				.collect(Collectors.toList());
	}

	protected void initFromIntervals(Interval ...intervals) {
		counts = new LinkedHashMap<>();
		for (var interval: intervals) {
			counts.put(interval, 0);
		}
	}

	/** prida jednu hodnotu do prislusneho intervalu histogramu */
	public void add(double value) {
		for (var interval : counts.keySet()) {
			if (interval.contains(value))
				counts.put(interval, counts.get(interval) + 1);
		}
	}
	
	/** prida vsechny zadaneho do odpovidajicich intervalu */
	public void addAll(Collection<Double> values) {
		values.forEach(this::add);
	}

	public DataSlicedHistogram(Attribute observed, int n) {
		super();
		if(!observed.domain.equals(Double.class)) {
			throw new RuntimeException(this.getClass().getName() + " can only observe " + Double.class.getName()
					+ " attributes. Given " + observed.domain.getName());
		}
		this.observed = observed;
		this.n = n;
	}

	/** vraci mnozinu intervalu, se kterymi histogram pracuje */
	public Set<Interval> intervals() {
		return Collections.unmodifiableSet(counts.keySet()); 
	}
	
	/** vraci pocet hodnot v danem intervalu */
	public Integer get(Interval interval) {
		return counts.get(interval);
	}
	
	public Integer get(RankInterval interval) {
		return this.get(new Interval(interval.start, interval.end, false, true));
	}
	
	/** vytvori prazdny histogram a naplni jej hodnotami */
	public static DataSlicedHistogram create(Interval[] intervals, Collection<Double> values,
			Attribute observed, int n) {
		var result = new EquidistantHistogram(observed, n);
		result.initFromIntervals(intervals);
		result.addAll(values);
		return result;
	}
	
	/** vraci celkovy pocet hodnot v historgramu */
	public int totalSize() {
		return counts.values().stream().mapToInt(a -> a).sum();
	}
}