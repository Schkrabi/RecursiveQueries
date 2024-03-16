package rq.common.statistic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
	
	protected DataSlicedHistogram(Attribute observed, int n, Map<Interval, Integer> counts) {
		super();
		this.observed = observed;
		this.n = n;
		this.counts = new HashMap<Interval, Integer>(counts);
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
	
	@Override
	public String toString() {
		return this.counts.toString();
	}
	
	public String serialize() {
		var sb = new StringBuilder();
		
		sb.append(this.n).append("\n");
		sb.append(this.observed.serialize()).append("\n");
		
		for(var e : this.counts.entrySet()) {
			sb.append(e.getKey().from).append(";")
				.append(e.getKey().closedFrom).append(";")
				.append(e.getKey().to).append(";")
				.append(e.getKey().closedTo).append(";")
				.append(e.getValue()).append("\n");
		}
		
		return sb.toString();
	}
	
	public void writeFile(String path) throws IOException {
		var serialized = this.serialize();
		Files.write(Path.of(path), serialized.getBytes());
	}
	
	public void writeFile(Path path) throws IOException {
		var serialized = this.serialize();
		Files.write(path, serialized.getBytes());
	}
	
	protected static class HistArgs {
		public final Attribute observed;
		public final int n;
		public final Map<Interval, Integer> counts;
		public HistArgs(Attribute observed, int n, Map<Interval, Integer> counts) {
			this.observed = observed;
			this.n = n;
			this.counts = counts;
		}
	}
	
	protected static HistArgs doDeserialize(String serialized) throws ClassNotFoundException{
		Attribute observed = null;
		Integer n = null;
		var counts = new LinkedHashMap<Interval, Integer>();
		
		for(var line : serialized.split("\n")) {
			if(n == null) {
				n = Integer.parseInt(line);
				continue;
			}
			if(observed == null) {
				observed = Attribute.parse(line);
				continue;
			}
			
			var parts = line.split(";");
			var from = Double.parseDouble(parts[0]);
			var closedFrom = Boolean.parseBoolean(parts[1]);
			var to = Double.parseDouble(parts[2]);
			var closedTo = Boolean.parseBoolean(parts[3]);
			var count = Integer.parseInt(parts[4]);
			
			counts.put(new Interval(from, to, closedFrom, closedTo), count);
		}
		
		return new HistArgs(observed, n, counts);
	}
}