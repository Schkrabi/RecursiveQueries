/**
 * 
 */
package rq.common.table;

/**
 * Represents expression that can yield a table
 * @author Mgr. R.Skrabal
 *
 */
public interface TabularExpression {
	/**
	 * Evaluates this expression and returns resulting table
	 * @return
	 */
	public Table eval();
	
	/**
	 * Returns the schema of table this tabular expression yields
	 * 
	 * @return Schema instance
	 * @remark Any implementations should not require evaluation of the expression
	 *         to get the schema. This method should be lightweight access to schema
	 *         for validations.
	 */
	public Schema schema();
}
