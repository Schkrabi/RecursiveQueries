package rq.common.similarities;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import rq.common.util.Pair;

public class NominalSimilarity<T> implements ISimilarity<T>{

	public record Entry<K, V>(K key, V value) {}
	
	private Map<Pair<T, T>, Double> simMap;
	
	public NominalSimilarity(
			Map<Pair<T, T>, Double> simMap) {
		this.simMap = new HashMap<>(simMap);
	}
	
	@JsonCreator
	public NominalSimilarity(
			@JsonProperty("map")
			List<Pair<Pair<T, T>, Double>> entries) {
		this.simMap = new HashMap<>();
		entries.stream().forEach(e -> this.simMap.put(e.first, e.second));
	}
	
	public List<Pair<Pair<T, T>, Double>> getMap(){
		return this.simMap.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue())).toList();
	}
	
	@Override
	public Double apply(T arg0, T arg1) {
		if(arg0.equals(arg1)) {
			return 1.0d;
		}
		var s = this.simMap.get(Pair.of(arg0, arg1));
		if(s == null) {
			s = this.simMap.get(Pair.of(arg1, arg0));
		}
		if(s == null) {
			return 0.0d;
		}
		
		return Math.max(Math.min(
				s, 
				1.0d), 
				0.0d);
	}
	
	@Override
	public int hashCode() {
		return this.simMap.hashCode();
	}
	
	@Override
	public boolean equals(Object other) {
		if(other instanceof NominalSimilarity ns) {
			return this.simMap.equals(ns.simMap);
		}
		return false;
	}

	@Override
	public String signature() {
		return "nomMap";
	}
}
