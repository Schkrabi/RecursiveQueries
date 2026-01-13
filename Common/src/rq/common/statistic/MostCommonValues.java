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
import rq.common.util.ISerilazeable;
import rq.common.util.NumberDeserializer;
import rq.common.util.Pair;

/** Observes the most common values on an attribute*/
public class MostCommonValues<T extends Number> implements IStatistic, ISerilazeable<MostCommonValues<T>> {
	
	private Map<T, Integer> counts = new HashMap<>();
	public final Attribute<T> observed;
	
	public MostCommonValues(Attribute<T> observed) {
		if(observed.domain != Double.class) {
			throw new RuntimeException("Must be Double attribute!");
		}
		this.observed = observed;
	}	

	private List<Pair<T, Integer>> _mostCommon;
	
	@Override
	public void gather(Table table) {
		for(var r : table) {
			try {
				var v = r.get(this.observed);
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
	public List<Pair<T, Integer>> mostCommon(int n){
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
	
	private double computeCenterOfGravity(Stream<Pair<T, Integer>> vlCntPairs) {
		return vlCntPairs.mapToDouble(p -> p.first.doubleValue() * (p.second.doubleValue() / this.total())).sum();
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
					.filter(e -> intv.contains(e.getKey().doubleValue()))
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
	
	public static <T extends Number> MostCommonValues<T> deserialize(String serialized, Class<T> type) {
		var data = new LinkedHashMap<T, Integer>();
		
		Attribute<T> observed = null;
		
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
			var key = NumberDeserializer.deserialize(vls[0], type);
			var value = Integer.parseInt(vls[1]);
			data.put(key, value);
		}
		
		var r = new MostCommonValues<>(observed);
		r.counts = data;
		r.recalculateMostCommon();
		return r;
	}
}
