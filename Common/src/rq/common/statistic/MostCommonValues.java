package rq.common.statistic;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.interfaces.Table;
import rq.common.statistic.DataSlicedHistogram.Interval;
import rq.common.table.Attribute;
import rq.common.util.DeserializerRegistry;
import rq.common.util.IDeserializer;
import rq.common.util.ISerilazeable;
import rq.common.util.Pair;

/** Observes the most common values on an attribute*/
public class MostCommonValues implements IStatistic, ISerilazeable<MostCommonValues> {

	static {
		DeserializerRegistry.register(MostCommonValues.class, new IDeserializer<MostCommonValues>() {

			@Override
			public MostCommonValues deserialize(String serialized) {
				return MostCommonValues.deserialize(serialized);
			}
		});
	}
	
	private Map<Double, Integer> counts = new HashMap<Double, Integer>();
	public final Attribute observed;
	
	public MostCommonValues(Attribute observed) {
		if(observed.domain != Double.class) {
			throw new RuntimeException("Must be Double attribute!");
		}
		this.observed = observed;
	}	

	private List<Pair<Double, Integer>> _mostCommon;
	
	@Override
	public void gather(Table table) {
		for(var r : table) {
			try {
				var v = (Double)r.get(this.observed);
				var cnt = this.counts.get(v);
				if(cnt == null) {
					cnt = 0;
				}
				this.counts.put(v, cnt + 1);
			} catch (AttributeNotInSchemaException e) {
				throw new RuntimeException(e);
			}
		}
		
		this.recalculateMostCommon();
	}
	
	private void recalculateMostCommon() {
		this._mostCommon = this.counts.entrySet().stream()
				.map(e -> Pair.of(e.getKey(), e.getValue()))
				.sorted((p1, p2) -> -Integer.compare(p1.second, p2.second))
				.collect(Collectors.toList());
	}

	/** Gets the list of most common values and their counts */
	public List<Pair<Double, Integer>> mostCommon(int n){
		return this._mostCommon.stream().limit(n).collect(Collectors.toList());
	}
	
	private Integer _total = null;
	/** Total number of tuples*/
	protected int total() {
		if(_total == null) {
			_total = this.counts.entrySet().stream()
					.mapToInt(e -> e.getValue()).sum();
		}
		return this._total.intValue();
	}
	
	private double computeCenterOfGravity(Stream<Pair<Double, Integer>> vlCntPairs) {
		return vlCntPairs.mapToDouble(p -> p.first * (p.second.doubleValue() / this.total())).sum();
	}
	
	//1 2 2 2 6 6 6 6
	//1 * 1/8 + 2 * 3/8 + 6 * 4/8 = 1/8 + 6/8 + 24/8 = 31/8 = 3 + 7/8 = 3.875	
	private Double _centerOfGravity = null;
	/**Global center of gravity, a weighted average of values */
	public double centerOfGravity() {
		if(this._centerOfGravity == null) {
			this._centerOfGravity = this.computeCenterOfGravity(
					this.counts.entrySet().stream()
					.map(e -> Pair.of(e.getKey(), e.getValue())));
		}
		return this._centerOfGravity.doubleValue();
	}
	
	private Map<Interval, Double> _centersOfGravity = new HashMap<>();
	/** Center of gravity for given interval*/
	public double centerOfGravity(Interval intv) {
		var cog = this._centersOfGravity.get(intv);
		if(cog == null) {
			cog = this.computeCenterOfGravity(this.counts.entrySet().stream()
					.filter(e -> intv.contains(e.getKey()))
					.map(e -> Pair.of(e.getKey(), e.getValue())));
			
			this._centersOfGravity.put(intv, cog);
		}
		return cog;
	}
	
	@Override
	public boolean equals(Object other) {
		if(other instanceof MostCommonValues mcv) {
			return this.observed.equals(mcv.observed);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return new StringBuilder()
				.append(this.observed.hashCode())
				.toString().hashCode();
	}

	@Override
	public String serialize() {
		var sb = new StringBuilder();
		
		sb.append(this.observed.serialize()).append("\n");
		
		for(var e : this.counts.entrySet()) {
			sb.append(e.getKey()).append(";")
				.append(e.getValue()).append("\n");
		}
		
		return sb.toString();
	}
	
	public static MostCommonValues deserialize(String serialized) {
		var data = new LinkedHashMap<Double, Integer>();
		
		Attribute observed = null;
		
		for(var line : serialized.split("\n")) {
			if(observed == null) {
				try {
					observed = Attribute.parse(line);
				} catch (ClassNotFoundException e) {
					throw new RuntimeException(e);
				}
				continue;
			}
			
			var vls = line.split(";");
			var key = Double.parseDouble(vls[0]);
			var value = Integer.parseInt(vls[1]);
			data.put(key, value);
		}
		
		var r = new MostCommonValues(observed);
		r.counts = data;
		r.recalculateMostCommon();
		return r;
	}
}
