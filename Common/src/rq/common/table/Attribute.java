package rq.common.table;

/**
 * Represents a column identifier
 * @author Mgr. R.Skrabal
 *
 */
public class Attribute {
	public final String name;
	public final Class<?> domain;
	
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
		return new StringBuilder()
				.append(this.name)
				.append(this.domain.toString())
				.toString()
				.hashCode();
	}
}
