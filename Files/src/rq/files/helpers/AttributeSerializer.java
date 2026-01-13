/**
 * 
 */
package rq.files.helpers;

import rq.common.table.Attribute;

/**
 * @author r.skrabal
 *
 */
public class AttributeSerializer<T> {
	private final Attribute<T> serialized;
	
	public AttributeSerializer(Attribute<T> serialized) {
		this.serialized = serialized;
	}
	
	public String serialize() {
		return this.serialized.serialize();
	}
	
	public static <T> String serialize(Attribute<T> serialized) {
		AttributeSerializer<T> serializer = new AttributeSerializer<>(serialized);
		String s = serializer.serialize();
		return s;
	}
}
