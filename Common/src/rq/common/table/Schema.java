/**
 * 
 */
package rq.common.table;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.HashMap;

/**
 * Represents database schema
 * @author Mgr. R.Skrabal
 *
 */
public class Schema {
	private final Map<Attribute, Integer> indexMap = new HashMap<Attribute, Integer>();
	
	public Schema(Attribute[] attributes) {
		for(int i = 0; i < attributes.length; i++) {
			this.indexMap.put(attributes[i], i);
		}
	}
	
	/**
	 * Gets index of given attribute.
	 * @param attribute
	 * @return Optional with index if the attribute is part of schema. Empty optional otherwise.
	 */
	Optional<Integer> attributeIndex(Attribute attribute) {
		Integer i = this.indexMap.get(attribute);
		if(i != null) {
			return Optional.of(i);
		}
		return Optional.empty();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		this.indexMap.entrySet().stream().sorted((e1, e2) -> e1.getValue().compareTo(e2.getValue()))
				.forEach(e -> sb.append(e.getKey().toString()));
		
		return sb.toString();
	}
	
	/**
	 * Gets stream of attributes in this Schema
	 * @return stream of attributes
	 */
	public Stream<Attribute> stream(){
		return this.indexMap.keySet().stream();
	}
	
	/**
	 * Gets stream of attribute - index pair in this Schema
	 * @return Stream with Attribute - Index Pair
	 */
	public Stream<Map.Entry<Attribute, Integer>> attrIndexStream(){
		return this.indexMap.entrySet().stream();
	}
	
	@Override
	public boolean equals(Object other) {
		if(!(other instanceof Schema)) {
			return false;
		}
		Schema s = (Schema)other;
		return this.indexMap.equals(s.indexMap);
	}
	
	@Override
	public int hashCode() {
		return this.indexMap.hashCode();
	}
}
