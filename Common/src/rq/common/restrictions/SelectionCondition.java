package rq.common.restrictions;

/**
 * Interface for condition on selection operations
 */
public interface SelectionCondition {
	/**
	 * Evaluates the condition on given record
	 * @param record record on which condition is evaluated. 
	 * @return Degree to which record satisfies the condition
	 * @remark the this.isApplicableToSchema(record.schema) must hold.
	 */
	public double eval(rq.common.table.Record record);
	
	/**
	 * Check if this condition is applicable to given schema.
	 * @param schema True if condition is applicable, false otherwise.
	 * @return true or false
	 */
	public boolean isApplicableToSchema(rq.common.table.Schema schema);
}
