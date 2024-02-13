package algorithmBuilder;

import java.util.function.Function;

import annotations.BuildsAlgorithm;
import rq.common.algorithms.LazyRecursiveTransformed_topKpruning;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.table.MemoryTable;
import rq.common.tools.AlgorithmMonitor;

@BuildsAlgorithm(LazyRecursiveTransformed_topKpruning.class)
public class AlgorithmBuilderTransformed_topKPruning extends AlgorithmBuilderTransformed {

	public AlgorithmBuilderTransformed_topKPruning(Function<Table, LazyExpression> initialExpressionProvider,
			Function<Table, Function<Table, LazyExpression>> recursiveExpressionProvider,
			Function<Table, LazyExpression> postprocessProvider, Integer k) {
		super(initialExpressionProvider, recursiveExpressionProvider, postprocessProvider, k);
	}
	
	@Override
	public TabularExpression buildMainQuery(Table iTable, AlgorithmMonitor monitor) {
		return LazyRecursiveTransformed_topKpruning.factory(
				this.initialExpressionProvider.apply(iTable), 
				this.recursiveExpressionProvider.apply(iTable), 
				this.k, 
				(rq.common.table.Record r) -> this.postprocessProvider.apply(MemoryTable.of(r)),
				monitor);				
	}

}
