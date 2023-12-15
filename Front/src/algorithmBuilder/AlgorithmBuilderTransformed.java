package algorithmBuilder;

import java.util.function.Function;

import annotations.BuildsAlgorithm;
import rq.common.algorithms.LazyRecursiveTransformed;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.table.MemoryTable;
import rq.common.tools.AlgorithmMonitor;

@BuildsAlgorithm(LazyRecursiveTransformed.class)
public class AlgorithmBuilderTransformed extends AlgorithmBuilder {

	public AlgorithmBuilderTransformed(
			Function<Table, LazyExpression> initialExpressionProvider,
			Function<Table, Function<Table, LazyExpression>> recursiveExpressionProvider,
			Function<Table, LazyExpression> postprocessProvider,
			Integer k) {
		super(initialExpressionProvider, recursiveExpressionProvider, postprocessProvider, k);
	}

	@Override
	public TabularExpression buildMainQuery(Table iTable, AlgorithmMonitor monitor) {
		return LazyRecursiveTransformed.factory(
				this.initialExpressionProvider.apply(iTable), 
				this.recursiveExpressionProvider.apply(iTable), 
				this.k, 
				(rq.common.table.Record r) -> this.postprocessProvider.apply(MemoryTable.of(r)),
				monitor);				
	}

	@Override
	public TabularExpression buildPostprocessQuery(Table iTable) {
		return iTable;
	}

}
