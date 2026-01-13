/**
 * 
 */
package rq.common.onOperators;

/**
 * @author Mgr. Radomir Skrabal
 *
 */
public abstract class OnCompare<T extends Comparable<T>> extends OnOperator<T> {

	protected OnCompare(RecordValue<T> left, RecordValue<T> right) {
		super(left, right);
	}
}
