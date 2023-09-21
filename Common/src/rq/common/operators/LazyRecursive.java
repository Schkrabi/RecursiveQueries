/**
 * 
 */
package rq.common.operators;

import java.util.function.Function;

import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.SchemaProvider;
import rq.common.table.Schema;
import rq.common.table.MemoryTable;

/**
 * This provides use of lazy expression for evaluation but the algorithm requires it to be eager.
 * @author Mgr. Radomir Skrabal
 *
 */
public abstract class LazyRecursive implements SchemaProvider {
	
	protected final LazyExpression argExp;
	protected final Function<LazyExpression, LazyExpression> funExpr;
	
	
	public abstract MemoryTable eval();
	
	protected LazyRecursive(
			LazyExpression argExp,
			Function<LazyExpression, LazyExpression> funExp) {
		this.argExp = argExp;
		this.funExpr = funExp;
	}

	@Override
	public Schema schema() {
		Schema schema = this.funExpr.apply(this.argExp).schema();
		return schema;
	}

}
