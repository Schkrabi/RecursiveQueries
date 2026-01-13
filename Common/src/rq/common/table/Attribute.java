package rq.common.table;

import rq.common.onOperators.RecordValue;

/**
 * Represents a column identifier
 * @author Mgr. R.Skrabal
 *
 */
public class Attribute<T> implements RecordValue<T>, Comparable<Attribute<?>>{
	public final String name;
	public final Class<T> domain;
	
	private boolean isHashCached = false;
	private int cachedHash = 0;
	
	public Attribute(String name, Class<T> domain) {
		this.name = name;
		this.domain = domain;
	}
	
	@SuppressWarnings("unchecked")
	public Attribute(String name, String domain) {
		this.name = name;
		try {
			this.domain = (Class<T>) Class.forName(domain);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Returns true if attributes have the same name. Returns false otherwise.
	 * @param other other attribute
	 * @return true or false
	 */
	public boolean equalsName(Attribute<?> other) {
		return this.name.equals(other.name);
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append(this.name)
				.append("(")
				.append(this.domain.getName())
				.append(")")
				.toString();
	}
	
	@Override
	public boolean equals(Object other) {
		if(other instanceof Attribute at) {
			return this.name.equals(at.name)
					&& this.domain.equals(at.domain);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		if(!this.isHashCached) {
			this.cachedHash =
					new StringBuilder()
					.append(this.name)
					.append(this.domain.toString())
					.toString()
					.hashCode();
			this.isHashCached = true;
		}
		
		return this.cachedHash;
	}

	@Override
	public T value(Record record) {
		return record.getNoThrow(this);
	}

	@Override
	public boolean isApplicableToSchema(Schema schema) {
		return schema.contains(this);
	}

	@Override
	public Class<T> domain() {
		return this.domain;
	}
	
	public static <U> Attribute<U> parse(String serialized) throws ClassNotFoundException {
		var pair = serialized.split(":");
		@SuppressWarnings("unchecked")
		var clazz = (Class<U>) Class.forName(pair[1]);
		return new Attribute<U>(pair[0], clazz);
	}
	
	public String serialize() {
		String s = new StringBuilder()
				.append(this.name)
				.append(":")
				.append(this.domain.getName())
				.toString();
		return s;
	}

	@Override
	public int compareTo(Attribute<?> o) {
		int cmp = this.name.compareTo(o.name);
		if(cmp != 0) {
			return cmp;
		}
		return this.domain.getName().compareTo(o.domain.getName());
	}
}
