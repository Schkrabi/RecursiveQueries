/**
 * 
 */
package rq.common.statistic;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

import rq.common.interfaces.Table;

/**
 * Histogram of ranks in the table
 */
public class RankHistogram extends SlicedStatistic {

	protected final Map<RankInterval, Double> rankHistogram;
	
	/**
	 * @param slices
	 */
	public RankHistogram(int slices) {
		super(uniformSlices(slices));
		this.rankHistogram = new HashMap<RankInterval, Double>();
		this.initData();
	}
	
	public RankHistogram(Set<RankInterval> slices) {
		super(slices);
		this.rankHistogram = new HashMap<RankInterval, Double>();
		this.initData();
	}
	
	public RankHistogram(Map<RankInterval, Double> rawHistogram) {
		super(rawHistogram.keySet());
		this.rankHistogram = new HashMap<RankInterval, Double>(rawHistogram);
	}
	
	protected void initData() {
		for(RankInterval i : this.slices) {
			this.rankHistogram.put(i, 0d);
		}
	}

	@Override
	public void gather(Table table) {
		this.initData();
		table.stream().forEach(r -> this.addRank(r.rank));
	}
	
	/**
	 * Gets the histogram value of given slice or 0 if such slice is not observed
	 * @param slice searched slice
	 * @return number of records in given slice or 0 if slice is not observed
	 */
	public double get(SlicedStatistic.RankInterval slice) {
		Double count = this.rankHistogram.get(slice);
		if(count == null) {
			return 0.d;
		}
		return count;
	}

	/**
	 * Gets the histogram value of given slice or 0 if such slice is not observed
	 * @param start start of searched slice (exclusive)
	 * @param end end of searched slice (inclusive)
	 * @return number of records in the slice or 0 if slice is not observed
	 */
	public double get(double start, double end) {
		return this.get(new SlicedStatistic.RankInterval(start, end));
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("RankHistogram:")
				.append(this.rankHistogram.toString())
				.toString();				
	}
	
	/**
	 * Finds rankInterval where the rank fits
	 * @param rank
	 * @return RankInterval instance observed by this statistic or null if none exists
	 */
	public RankInterval fit(double rank) {
		Iterator<RankInterval> it = this.getSlices().iterator();
		while(it.hasNext()) {
			RankInterval interval = it.next();
			if(interval.isMember(rank)) {
				return interval;
			}
		}
		return null;
	}
	
	/**
	 * Adds two histograms
	 * @param left
	 * @param right
	 * @return new RankHistogram instance
	 */
	public static RankHistogram add(RankHistogram left, RankHistogram right) {
		if(!left.slices.equals(right.slices)) {
			throw new RuntimeException("Only rank histograms with equal slices can be added.");
		}
		
		Map<RankInterval, Double> rslt = new HashMap<RankInterval, Double>();
		
		for(Map.Entry<RankInterval, Double> e : left.rankHistogram.entrySet()) {
			RankInterval interval = e.getKey();
			double leftValue = e.getValue();
			double rightValue = right.get(interval);
			
			rslt.put(interval, leftValue + rightValue);
		}
		
		return new RankHistogram(rslt);
	}
	
	/**
	 * Multiplies each class of rank hustogram by number
	 * @param divident
	 * @param denominator
	 * @return new RankHistogram instance
	 */
	public static RankHistogram mult(RankHistogram left, double right) {
		
		Map<RankInterval, Double> rslt = new HashMap<RankInterval, Double>();
		
		for(Map.Entry<RankInterval, Double> e : left.rankHistogram.entrySet()) {
			RankInterval interval = e.getKey();
			double value = e.getValue();
			
			rslt.put(interval, value * right);
		}
		
		return new RankHistogram(rslt);
	}
	
	public Map<RankInterval, Double> get(){
		return new HashMap<RankInterval, Double>(this.rankHistogram);
	}
	
	public void setIntervalValue(RankInterval interval, double value) {
		if(this.slices.contains(interval)) {
			this.rankHistogram.put(interval, Double.valueOf(value));
		}
	}
	
	public void addIntervalValue(RankInterval interval, double value) {
		if(this.slices.contains(interval)) {
			this.rankHistogram.put(interval, this.rankHistogram.get(interval) + value);
		}
	}
	
	/**
	 * Adds one occurrence of given rank
	 * @param rank
	 */
	public void addRank(double rank) {
		RankInterval interval = this.fit(rank);
		if(interval != null) {
			this.addIntervalValue(interval, 1);
		}
	}
	
	/**
	 * Adds all ranks and fits them into the histogram
	 * @param ranks
	 */
	public void addRanks(Collection<Double> ranks) {
		ranks.forEach(r -> this.addRank(r));
	}
	
	public double tableSize() {
		return this.rankHistogram.values().stream().reduce(0d, (x, y) -> x + y);
	}
		
	public static RankHistogram build(Collection<Double> values, int n) {
		var hist = new RankHistogram(n);
		hist.addRanks(values);
		return hist;
	}
	
	public String serialize() {
		var sb = new StringBuilder();
		
		for(var e : this.rankHistogram.entrySet()) {
			sb.append(e.getKey().start)
				.append(";")
				.append(e.getKey().end)
				.append(";")
				.append(e.getValue())
				.append("\n");
			
		}
		
		return sb.toString();
	}
	
	public static RankHistogram deserialize(String data) {
		var hist = new LinkedHashMap<RankInterval, Double>();
		
		for(var line : data.split("\n")) {
			var vls = line.split(";");
			var start = Double.parseDouble(vls[0]);
			var end = Double.parseDouble(vls[1]);
			var count = Double.parseDouble(vls[2]);
			hist.put(new RankInterval(start, end), count);
		}
		
		return new RankHistogram(hist);
	}
	
	public void writeFile(String path) throws IOException {
		writeFile(Path.of(path));
	}
	
	public void writeFile(Path path) throws IOException {
		Files.write(path, this.serialize().getBytes());
	}
	
	public static RankHistogram readFile(Path path) throws IOException {
		return deserialize(Files.readString(path));
	}
	
	public static RankHistogram readFile(String path) throws IOException {
		return readFile(Path.of(path));
	}
}
