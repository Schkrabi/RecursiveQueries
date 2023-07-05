package rq.common.exceptions;

import java.util.Collection;
import java.util.ArrayList;

import rq.common.table.Schema;

/**
 * Raised when user tries to create record with values of different types than specified by schema
 * @author Mgr. R.Skrabal
 *
 */
public class TypeSchemaMismatchException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 919677572195361993L;
	public final Schema schema;
	public final Collection<Class<?>> domains;
	
	public TypeSchemaMismatchException(Schema schema, Collection<Class<?>> types) {
		super(new StringBuilder()
				.append("Value types ")
				.append(types)
				.append(" do not match schema ")
				.append(schema)
				.toString());
		this.schema = schema;
		this.domains = new ArrayList<Class<?>>(types);
	}
}
