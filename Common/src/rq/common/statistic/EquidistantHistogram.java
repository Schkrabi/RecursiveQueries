package rq.common.statistic;

import java.util.List;

import rq.common.interfaces.Table;
import rq.common.table.Attribute;

public class EquidistantHistogram extends DataSlicedHistogram {

	public EquidistantHistogram(Attribute observed, int n) {
		super(observed, n);
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
}
