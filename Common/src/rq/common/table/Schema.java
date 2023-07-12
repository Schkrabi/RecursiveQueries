/**
 * 
 */
package rq.common.table;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import rq.common.exceptions.DuplicateAttributeNameException;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Represents database schema
 * @author Mgr. R.Skrabal
 *
 */
public class Schema implements Iterable<Attribute> {
	private final Map<Attribute, Integer> indexMap = new HashMap<Attribute, Integer>();
	private final Map<String, Integer> nameMap = new HashMap<String, Integer>();
	
	private Schema(Collection<Attribute> attributes) {
		int i = 0;
		for(Attribute at : attributes.stream().sorted().collect(Collectors.toList())) {
			this.indexMap.put(at, i);
			this.nameMap.put(at.name, i);
			i++;
		}
	}
	
	/**
	 * Finds name duplicates in attribute collection.
	 * @param attributes checked attribute collection
	 * @return Optional with name, if duplicate detected. Empty otional otherwise
	 */
	private static Optional<String> findDuplicateAttributeName(Collection<Attribute> attributes){
		Set<String> names = new HashSet<String>();
		for(Attribute a : attributes){
			if(names.contains(a.name)) {
				return Optional.of(a.name);
			}
			names.add(a.name);
		}
		return Optional.empty();
	}
	
	/**
	 * Validates collection of attributes for duplicate attribute names
	 * @param attributes checked attribute collection
	 * @return true if no duplicates found, false otherwise
	 */
	private static boolean validateAttributeCollection(Collection<Attribute> attributes)
			throws DuplicateAttributeNameException {
		Optional<String> duplicate = Schema.findDuplicateAttributeName(attributes);
		if(duplicate.isPresent()) {
			throw new DuplicateAttributeNameException(duplicate.get());
		}
		return true;
	}
	
	/**
	 * Creates new instance of Schema
	 * @param attributes attributes of the schema
	 * @return new Schema instance
	 * @throws DuplicateAttributeNameException if arguments does not validate
	 */
	public static Schema factory(Collection<Attribute> attributes)
		throws DuplicateAttributeNameException {
		if(Schema.validateAttributeCollection(attributes)) {
			 return new Schema(attributes);
		}
		//Effectivelly unreacheable
		return null;
	}
	
	/**
	 * Creates new instance of Schema
	 * @param attributes attributes of the schema
	 * @return Schema instance 
	 * @throws DuplicateAttributeNameException if arguments does not validate
	 */
	public static Schema factory(Attribute ...attributes) 
			throws DuplicateAttributeNameException {
		return Schema.factory(Arrays.asList(attributes));
	}
	
	/**
	 * Gets index of given attribute.
	 * @param attribute
	 * @return Optional with index if the attribute is part of schema. Empty optional otherwise.
	 */
	public Optional<Integer> attributeIndex(Attribute attribute) {
		Integer i = this.indexMap.get(attribute);
		if(i != null) {
			return Optional.of(i);
		}
		return Optional.empty();
	}
	
	/**
	 * Gets index of given attribute name
	 * @param attribName
	 * @return Optional with index if the attribute with such name is part of schema. Empty optional otherwise.
	 */
	public Optional<Integer> attributeIndex(String attribName) {
		Integer i = this.nameMap.get(attribName);
		if(i != null) {
			return Optional.of(i);
		}
		return Optional.empty();
	}
	
	@Override
	public String toString() {
		return
			this.indexMap.entrySet().stream().sorted((e1, e2) -> e1.getValue().compareTo(e2.getValue()))
					.map(a -> a.getKey().toString())
					.reduce((s1, s2) -> s1 + ", " + s2)
					.get();
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
	
	/**
	 * Gets number of attributes in the schema
	 * @return integer
	 */
	public int size() {
		return this.indexMap.size();
	}
	
	/**
	 * Returns true if given schema is subset of this schema. Returns true otherwise.
	 * @param subschema inspected subschema
	 * @return True or false
	 */
	public boolean isSubSchema(Schema subschema) {
		return this.indexMap.keySet().containsAll(subschema.indexMap.keySet());
	}

	@Override
	public Iterator<Attribute> iterator() {
		return this.indexMap.keySet().iterator();
	}
	
	/**
	 * Returns true if attribute belong to this schema, otherwise returns false.
	 * @param attribute checked attribute
	 * @return true or false.
	 */
	public boolean contains(Attribute attribute) {
		return this.indexMap.containsKey(attribute);
	}
	
	/**
	 * Returns true if attribute with given name belongs to this schema. Returns false otherwise.
	 * @param attributeName inspected name
	 * @return true or false
	 */
	public boolean contains(String attributeName) {
		return this.nameMap.containsKey(attributeName);
	}
	
	/**
	 * Returns false if this and schema has attribute with same name but with different domain. Otherwise returns true.
	 * @param schema other schema
	 * @return true or false
	 */
	public boolean isJoinableWith(Schema schema) {
		for(Attribute a : this) {
			if(schema.contains(a.name) && !schema.contains(a)) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Gets set of all attributes in this schema
	 * @return set of attributes.
	 */
	public Set<Attribute> attributeSet(){
		return this.indexMap.keySet();
	}
}
