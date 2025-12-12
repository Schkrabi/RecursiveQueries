package rq.common.statistic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import rq.common.estimations.SignatureProvider;
import rq.common.interfaces.Table;
import rq.common.table.Attribute;

public class EquinominalHistogram extends DataSlicedHistogram implements SignatureProvider  {

	public EquinominalHistogram(Attribute observed, int n) {
		super(observed, n);
	}
	
	private EquinominalHistogram(Attribute observed, int n, Map<Interval, Integer> counts) {
		super(observed, n, counts);
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

	public static EquinominalHistogram deserialize(String serialized) throws ClassNotFoundException {
		var args = DataSlicedHistogram.doDeserialize(serialized);
		var hist = new EquinominalHistogram(args.observed, args.n, args.counts);
		return hist;
	}
	
	public static EquinominalHistogram readFile(String path) {
		return readFile(Path.of(path));
	}
	
	public static EquinominalHistogram readFile(Path path) {
		EquinominalHistogram hist;
		try {
			hist = deserialize(Files.readString(path));
		} catch (ClassNotFoundException | IOException e) {
			throw new RuntimeException(e);
		}
		return hist;
	}

	@Override
	public String signature() {
		return "Eqn";
	}
}
