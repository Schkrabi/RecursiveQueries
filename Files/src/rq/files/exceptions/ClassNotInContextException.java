package rq.files.exceptions;

public class ClassNotInContextException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1409690171694212615L;
	public final Object context;
	public final Class<?> targetClass;
	
	public ClassNotInContextException(Object context, Class<?> targetClass) {
		super(new StringBuilder()
				.append("Context ")
				.append(context.toString())
				.append(" does not contain parsing function for class ")
				.append(targetClass.toString())
				.toString());
		this.context = context;
		this.targetClass = targetClass;
	}
}
