package algorithmBuilder;

import java.util.function.Function;

import annotations.BuildsAlgorithm;
import rq.common.algorithms.LazyRecursiveUnrestricted;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.table.TopKTable;
import rq.common.tools.AlgorithmMonitor;

@BuildsAlgorithm(LazyRecursiveUnrestricted.class)
public class AlgorithmBuilderUnrestricted extends AlgorithmBuilder {
	
	public AlgorithmBuilderUnrestricted(Function<Table, LazyExpression> initialExpressionProvider,
			Function<Table, Function<Table, LazyExpression>> recursiveExpressionProvider,
			Function<Table, LazyExpression> postprocessProvider,
			Integer k) {
		super(initialExpressionProvider, recursiveExpressionProvider, postprocessProvider, k);
	}

	@Override
	public TabularExpression buildMainQuery(Table iTable, AlgorithmMonitor monitor) {
		TabularExpression exp = null;
		
		exp = LazyRecursiveUnrestricted.factory(
				this.initialExpressionProvider.apply(iTable), 
				this.recursiveExpressionProvider.apply(iTable),
				monitor);
		
		return exp;
	}

	@Override
	public TabularExpression buildPostprocessQuery(Table iTable) {
		return TopKTable.realizer(
				this.postprocessProvider.apply(iTable), 
				this.k);
	}

}
