/**
 * 
 */
package rq.common.onOperators;

import rq.common.table.Record;
import rq.common.table.Schema;

/**
 * Abstract class for operators of ON clauses
 * @author Mgr. Radomir Skrabal
 *
 */
public abstract class OnOperator {
	public final RecordValue left;
	public final RecordValue right;
	
	protected OnOperator(RecordValue left, RecordValue right) {
		this.left = left;
		this.right = right;
	}
	
	/**
	 * Evaluates the operator on a pair of records
	 * @param leftRecord 
	 * @param rightRecord
	 * @return true if this on clause operator is fulfilled, false otherwise
	 */
	public abstract double eval(Record leftRecord, Record rightRecord);
	
	public boolean isApplicableToSchema(Schema leftSchema, Schema rightSchema) {
		return this.left.isApplicableToSchema(leftSchema)
				&& this.right.isApplicableToSchema(rightSchema);
	}
}
