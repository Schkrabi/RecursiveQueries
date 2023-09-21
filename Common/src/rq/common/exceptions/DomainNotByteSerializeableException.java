/**
 * 
 */
package rq.common.exceptions;

import rq.common.interfaces.ByteArraySerializable;

/**
 * @author r.skrabal
 *
 */
public class DomainNotByteSerializeableException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3998377327518138784L;
	public final Class<?> domain;
	
	public DomainNotByteSerializeableException(Class<?> domain) {
		super(new StringBuilder()
				.append("Domain must implement ")
				.append(ByteArraySerializable.class.getName())
				.append(" interface.")
				.toString());
		this.domain = domain;
	}
	
	
}
