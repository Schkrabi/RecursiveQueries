package rq.common.table;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.Set;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.TypeSchemaMismatchException;

/**
 * Represents a single record
 * 
 * @author Mgr. R.Skrabal
 *
 */
public class Record {

	public record AttributeValuePair(Attribute attribute, Object value) {
	}

	public final Schema schema;
	private final Object[] values;
	public final Double rank;

	private Record(Schema schema, Object[] values, Double rank) {
		this.schema = schema;
		this.values = values;
		this.rank = rank;
	}
	
	/**
	 * Constructor for copying values and changing rank
	 * @param record
	 * @param rank
	 */
	public Record(Record record, double rank) {
		this.schema = record.schema;
		this.values = Arrays.copyOf(record.values, record.values.length);
		this.rank = rank;
	}

	/**
	 * Validates values against the schema domains
	 * 
	 * @param schema
	 * @param values
	 * @return true if all domains are correct, false otherwise
	 */
	private static boolean validateValuesToSchema(Schema schema, Object[] values) {
		return schema.size() == values.length
				&& schema.attrIndexStream().allMatch(e -> e.getKey().domain.equals(values[e.getValue()].getClass()));
	}

	/**
	 * Factory method
	 * 
	 * @param schema schema of the record
	 * @param values values of the record
	 * @param rank   rank of the record
	 * @return Record instance
	 * @throws TypeSchemaMismatchException if types of values mismatch Schema
	 */
	private static Record factory(Schema schema, Object[] values, double rank) throws TypeSchemaMismatchException {
		if (!validateValuesToSchema(schema, values)) {
			throw new TypeSchemaMismatchException(schema,
					Arrays.asList(values).stream().map(x -> x.getClass()).collect(Collectors.toList()));
		}
		return new Record(schema, values, rank);
	}

	public static Record factory(Schema schema, Collection<AttributeValuePair> values, double rank)
			throws TypeSchemaMismatchException, AttributeNotInSchemaException {
		// Add missing values
		Set<AttributeValuePair> s = new HashSet<AttributeValuePair>(values);
		for (Attribute a : schema) {
			if (values.stream().filter(p -> p.attribute.equals(a)).findAny().isEmpty()) {
				s.add(new AttributeValuePair(a, null));
			}
		}
		// Check if there are extra values
		if (s.size() > schema.size()) {
			throw new AttributeNotInSchemaException(
					s.stream().filter(p -> !schema.contains(p.attribute)).findAny().get().attribute, schema);
		}

		return Record.factory(schema,
				values.stream().sorted((p1, p2) -> p1.attribute.compareTo(p2.attribute)).map(p -> p.value).toArray(),
				rank);
	}

	public static Record factory(Schema schema, double rank, AttributeValuePair... values)
			throws TypeSchemaMismatchException, AttributeNotInSchemaException {
		return Record.factory(schema, Arrays.asList(values), rank);
	}

	/**
	 * Gets the value of an attribute in this record
	 * 
	 * @param attribute the attribute
	 * @return Object value
	 * @throws AttributeNotInSchemaException if attribute is not part of the schema
	 */
	public Object get(Attribute attribute) throws AttributeNotInSchemaException {
		Optional<Integer> index = this.schema.attributeIndex(attribute);
		if (index.isEmpty()) {
			throw new AttributeNotInSchemaException(attribute, this.schema);
		}
		return this.values[index.get()];
	}

	/**
	 * Gets the value of an attribute given by name in this record
	 * 
	 * @param name name of the attribute
	 * @return Object value
	 * @throws AttributeNotInSchemaException if attribute with that name is not part
	 *                                       of the schema
	 */
	public Object get(String name) throws AttributeNotInSchemaException {
		Optional<Integer> index = this.schema.attributeIndex(name);
		if (index.isEmpty()) {
			throw new AttributeNotInSchemaException(name, this.schema);
		}
		return this.values[index.get()];
	}
	
	/**
	 * Gets the value of an attribute. If attribute is not part of the schema returns null.
	 * @param attribute searched attribute
	 * @return Object or null.
	 */
	public Object getNoThrow(Attribute attribute) {
		Optional<Integer> index = this.schema.attributeIndex(attribute);
		if (index.isEmpty()) {
			return null;
		}
		return this.values[index.get()];
	}
	
	/**
	 * Gets the value of an attribute by name. If attribute is not part of the schema returns null.
	 * @param name searched attribute name
	 * @return Object or null
	 */
	public Object getNoThrow(String name) {
		Optional<Integer> index = this.schema.attributeIndex(name);
		if (index.isEmpty()) {
			return null;
		}
		return this.values[index.get()];
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append("[")
				.append(this.schema.attrIndexStream()
						.map(e -> e.getKey().toString() + ": " + this.values[e.getValue()].toString())
						.reduce((s1, s2) -> s1 + ", " + s2).get())
				.append("; ")
				.append(this.rank)
				.append("]")
				.toString();
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof Record)) {
			return false;
		}
		Record r = (Record) other;
		if (!this.schema.equals(r.schema) || !this.rank.equals(r.rank)) {
			return false;
		}

		for (int i = 0; i < this.values.length; i++) {
			if (!this.values[i].equals(r.values[i])) {
				return false;
			}
		}

		return true;
	}

	@Override
	public int hashCode() {
		return new StringBuilder().append(this.schema)
				.append(Arrays.stream(this.values).map(x -> x.toString()).reduce((x, y) -> x + y)).append(this.rank)
				.toString().hashCode();
	}
	
	/**
	 * Returns true if other record is equal to this, excluding rank. Returns false otherwise.
	 * @param other
	 * @return true or false
	 */
	public boolean equalsNoRank(Record other) {
		if (!this.schema.equals(other.schema)) {
			return false;
		}
		//If schemas are equal, the value arrays must also be equal
		for (int i = 0; i < this.values.length; i++) {
			if (!this.values[i].equals(other.values[i])) {
				return false;
			}
		}

		return true;
	}
}
