package rq.common.table;

/**
 * Represents a single record
 * @author Mgr. R.Skrabal
 *
 */
public class Record {
	public final Schema schema;
	public final Object[] values;
	public final Double rank;
	
	public Record(Schema schema, Object[] values, Double rank) {
		this.schema = schema;
		this.values = values;
		this.rank = rank;
	}
	
	/**
	 * Gets the value of an attribute in this record
	 * @param attribute
	 * @return
	 */
	public Object get(Attribute attribute) {
		return this.values[this.schema.attributeIndex(attribute).get()];
	}
	
	@Override
	public String toString() {
		return this.schema.attrIndexStream()
				.map(e -> e.getKey().toString() + ": " + this.values[e.getValue()].toString())
				.reduce(new StringBuilder(), (sb, s) -> sb.append(s), (sb1, sb2) -> sb1.append(sb2.toString()))
				.toString();
	}
	
	@Override
	public boolean equals(Object other) {
		if(!(other instanceof Record)) {
			return false;
		}
		Record r = (Record)other;
		return this.schema.equals(r.schema)
				&& this.values.equals(r.values)
				&&	this.rank.equals(r.rank);
	}
	
	@Override
	public int hashCode() {
		return new StringBuilder()
				.append(this.schema.hashCode())
				.append(this.values.hashCode())
				.append(this.rank.hashCode())
				.toString()
				.hashCode();
	}
}
