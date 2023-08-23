/**
 * 
 */
package rq.common.onOperators;

import rq.common.table.Attribute;
import rq.common.table.Record;

/**
 * Abstract class for operators of ON clauses
 * @author Mgr. Radomir Skrabal
 *
 */
public abstract class OnOperator {
	public final Attribute left;
	public final Attribute right;
	
	protected OnOperator(Attribute left, Attribute right) {
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
}
