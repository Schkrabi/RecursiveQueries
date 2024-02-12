/**
 * 
 */
package rq.common.exceptions;

import rq.common.restrictions.SelectionCondition;
import rq.common.table.Schema;

/**
 * 
 */
public class SelectionConditionNotApplicableToSchema extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2570399061921387246L;
	public final SelectionCondition condition;
	public final Schema schema;
	
	/**
	 * 
	 */
	public SelectionConditionNotApplicableToSchema(SelectionCondition condition, Schema schema) {
		super(new StringBuilder()
				.append("Selection condition ")
				.append(condition)
				.append(" is not applicable to schema ")
				.append(schema)
				.toString());
		this.condition = condition;
		this.schema = schema;
	}

}
