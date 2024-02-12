package rq.common.statistic;

import java.util.LinkedHashMap;
import java.util.TreeMap;

import rq.common.interfaces.Table;
import rq.common.table.Attribute;

public class EquinominalHistogra extends DataSlicedHistogram {

	public EquinominalHistogra(Attribute observed, int n) {
		super(observed, n);
	}

	@Override
	public void gather(Table table) {
		var values = this.values(table);

		var cts = new TreeMap<Double, Integer>();
		for (var v : values) {
			int count = cts.getOrDefault(v, 0);
			cts.put(v, count + 1);
		}
		
		int minimalIntervalSize = values.size() / n;
		
		this.counts = new LinkedHashMap<Interval, Integer>();
		var it = cts.keySet().iterator();
		
		Double start = it.next();
		int count = cts.get(start);
		Double next = null;
		while (it.hasNext()) {
			next = it.next();
			if (count > minimalIntervalSize) {
				this.counts.put(new Interval(start.doubleValue(), next.doubleValue(), true, false), count);
				start = next;
				count = 0;
			}
			count += cts.get(next);
		}
		this.counts.put(new Interval(start.doubleValue(), next.doubleValue(), true, true), count);
	}

}
