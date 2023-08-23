/**
 * 
 */
package rq.common.exceptions;

import rq.common.table.Attribute;

/**
 * Exception thrown if <, <=, >, >= are used with attributes without comparable domains
 * @author Mgr. Radomir Skrabal
 *
 */
public class NotComparableException extends Exception{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5866038504535603707L;
	public final Attribute attribute;
	public final Class<?> operator;
	
	public NotComparableException(Attribute attribute, Class<?> operator) {
		super(new StringBuilder()
				.append("Not comparable domain ")
				.append(attribute.domain.getName())
				.append(" of attribute ")
				.append(attribute.toString())
				.append(" used in ")
				.append(operator.getName())
				.toString());
		this.attribute = attribute;
		this.operator = operator;		
	}
}
