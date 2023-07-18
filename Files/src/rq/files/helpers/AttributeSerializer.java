/**
 * 
 */
package rq.files.helpers;

import rq.common.table.Attribute;

/**
 * @author r.skrabal
 *
 */
public class AttributeSerializer {
	private final Attribute serialized;
	
	public AttributeSerializer(Attribute serialized) {
		this.serialized = serialized;
	}
	
	public String serialize() {
		String s = new StringBuilder()
				.append(serialized.name)
				.append(":")
				.append(serialized.domain.getName())
				.toString();
		return s;
	}
	
	public static String serialize(Attribute serialized) {
		AttributeSerializer serializer = new AttributeSerializer(serialized);
		String s = serializer.serialize();
		return s;
	}
}
