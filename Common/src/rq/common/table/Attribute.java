package rq.common.table;

import rq.common.onOperators.RecordValue;

/**
 * Represents a column identifier
 * @author Mgr. R.Skrabal
 *
 */
public class Attribute implements Comparable<Attribute>, RecordValue{
	public final String name;
	public final Class<?> domain;
	
	private boolean isHashCached = false;
	private int cachedHash = 0;
	
	public Attribute(String name, Class<?> domain) {
		this.name = name;
		this.domain = domain;
	}
	
	/**
	 * Returns true if attributes have the same name. Returns false otherwise.
	 * @param other other attribute
	 * @return true or false
	 */
	public boolean equalsName(Attribute other) {
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
		if(!(other instanceof Attribute)) {
			return false;
		}
		return this.name.equals(((Attribute)other).name)
				&& this.domain.equals((((Attribute)other).domain));
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
	public int compareTo(Attribute o) {
		int cmp = this.name.compareTo(o.name);
		if(cmp != 0) {
			return cmp;
		}
		return this.domain.getName().compareTo(o.domain.getName());
	}

	@Override
	public Object value(Record record) {
		return record.getNoThrow(this);
	}

	@Override
	public boolean isApplicableToSchema(Schema schema) {
		return schema.contains(this);
	}

	@Override
	public Class<?> domain() {
		return this.domain;
	}
}
