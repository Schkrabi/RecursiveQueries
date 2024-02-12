/**
 * 
 */
package rq.common.exceptions;

import rq.common.onOperators.RecordValue;

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
	public final RecordValue recordValue;
	public final Class<?> operator;
	
	public NotComparableException(RecordValue recordValue, Class<?> operator) {
		super(new StringBuilder()
				.append("Not comparable domain ")
				.append(recordValue.domain().getName())
				.append(" of attribute ")
				.append(recordValue.toString())
				.append(" used in ")
				.append(operator.getName())
				.toString());
		this.recordValue = recordValue;
		this.operator = operator;		
	}
}
