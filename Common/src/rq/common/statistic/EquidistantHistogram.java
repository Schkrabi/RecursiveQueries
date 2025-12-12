package rq.common.statistic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import rq.common.estimations.SignatureProvider;
import rq.common.interfaces.Table;
import rq.common.table.Attribute;

public class EquidistantHistogram extends DataSlicedHistogram implements SignatureProvider {

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
		
		//double dist = (max - min) / this.n;

		Interval[] intervals = new Interval[n]; 
		//double from = min;
		double range = max - min;
		
		for (int i = 0; i < n; i++) {
			//double from = min + dist * i; 
			//	var to = i == (n-1) ? max : from + dist;
			double from = Math.fma(range, (double) i / n, min); // fused multiply-add
		    double to   = (i == n - 1) ? max : Math.fma(range, (double) (i + 1) / n, min);
			intervals[i] = new Interval(from, to, true, i == (n - 1));
			//from = to;
		}
		
		this.initFromIntervals(intervals);
		values.forEach(v -> this.add(v.doubleValue()));
	}
	
	public static EquidistantHistogram deserialize(String serialized) throws ClassNotFoundException {
		var args = DataSlicedHistogram.doDeserialize(serialized);
		var hist = new EquidistantHistogram(args.observed, args.n, args.counts);
		return hist;
	}
	
	public static EquidistantHistogram readFile(String path) {
		return readFile(Path.of(path));
	}
	
	public static EquidistantHistogram readFile(Path path) {
		EquidistantHistogram hist;
		try {
			hist = deserialize(Files.readString(path));
		} catch (ClassNotFoundException | IOException e) {
			throw new RuntimeException(e);
		}
		return hist;
	}

	@Override
	public String signature() {
		return "Eqd";
	}
}
