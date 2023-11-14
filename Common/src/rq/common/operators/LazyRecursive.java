/**
 * 
 */
package rq.common.operators;

import java.util.function.Function;

import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.table.Schema;
import rq.common.tools.Counter;

/**
 * This provides use of lazy expression for evaluation but the algorithm requires it to be eager.
 * @author Mgr. Radomir Skrabal
 *
 */
public abstract class LazyRecursive implements TabularExpression {
	
	protected final LazyExpression argExp;
	protected final Function<Table, LazyExpression> funExpr;
	
	protected final Counter recordCounter;
	
	protected LazyRecursive(
			LazyExpression argExp,
			Function<Table, LazyExpression> funExp,
			Counter recordCounter) {
		this.argExp = argExp;
		this.funExpr = funExp;
		this.recordCounter = recordCounter;
	}

	@Override
	public Schema schema() {
		Schema schema = this.argExp.schema();
		return schema;
	}
	
	protected void incrementCounter() {
		if(this.recordCounter != null) {
			this.recordCounter.increment();
		}
	}

}
