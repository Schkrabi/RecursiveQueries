/**
 * 
 */
package rq.common.onOperators;

import rq.common.exceptions.ComparableDomainMismatchException;
import rq.common.exceptions.NotComparableException;
import rq.common.table.Attribute;

/**
 * @author Mgr. Radomir Skrabal
 *
 */
public abstract class OnCompare extends OnOperator {

	protected OnCompare(Attribute left, Attribute right) {
		super(left, right);
	}

	protected static boolean validateComparable(Attribute left, Attribute right)
			throws NotComparableException, ComparableDomainMismatchException {
		if (!Comparable.class.isAssignableFrom(left.domain)) {
			throw new NotComparableException(left, OnGreaterThan.class);
		}
		if (!Comparable.class.isAssignableFrom(right.domain)) {
			throw new NotComparableException(right, OnGreaterThan.class);
		}
		if (!left.domain.equals(right.domain)) {
			throw new ComparableDomainMismatchException(left, right);
		}

		return true;
	}
}
