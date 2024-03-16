package rq.common.statistic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import rq.common.interfaces.Table;
import rq.common.table.Attribute;

public class EquidistantHistogram extends DataSlicedHistogram {

	public EquidistantHistogram(Attribute observed, int n) {
		super(observed, n);
	}
	
	private EquidistantHistogram(Attribute observed, int n, Map<Interval, Integer> counts) {
		super(observed, n, counts);
	}
	
	@Override
	public void gather(Table table) {
		List<Double> values = this.values(table);
		
		double min = values.stream().mapToDouble(Number::doubleValue).min().getAsDouble();
		double max = values.stream().mapToDouble(Number::doubleValue).max().getAsDouble();
		
		double dist = (max - min) / this.n;

		Interval[] intervals = new Interval[n]; 
		for (int i = 0; i < n; i++) {
			double from = min + dist * i; 
			intervals[i] = new Interval(from, from + dist, true, i == (n - 1));
		}
		
		this.initFromIntervals(intervals);
		values.forEach(v -> this.add(v.doubleValue()));
	}
	
	public static EquidistantHistogram deserialize(String serialized) throws ClassNotFoundException {
		var args = DataSlicedHistogram.doDeserialize(serialized);
		var hist = new EquidistantHistogram(args.observed, args.n, args.counts);
		return hist;
	}
	
	public static EquidistantHistogram readFile(String path) throws ClassNotFoundException, IOException {
		return readFile(Path.of(path));
	}
	
	public static EquidistantHistogram readFile(Path path) throws ClassNotFoundException, IOException {
		var hist = deserialize(Files.readString(path));
		return hist;
	}
}
