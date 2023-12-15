package algorithmBuilder;

import java.util.function.Function;

import annotations.BuildsAlgorithm;
import rq.common.algorithms.LazyRecursiveTopK;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.tools.AlgorithmMonitor;

@BuildsAlgorithm(LazyRecursiveTopK.class)
public class AlgorithmBuilderTopK extends AlgorithmBuilder {

	public AlgorithmBuilderTopK(Function<Table, LazyExpression> initialExpressionProvider,
			Function<Table, Function<Table, LazyExpression>> recursiveExpressionProvider,
			Function<Table, LazyExpression> postprocessProvider, Integer k) {
		super(initialExpressionProvider, recursiveExpressionProvider, postprocessProvider, k);
	}

	@Override
	public TabularExpression buildMainQuery(Table iTable, AlgorithmMonitor monitor) {
		TabularExpression exp = null;

		exp = LazyRecursiveTopK.factory(
				this.initialExpressionProvider.apply(iTable), 
				this.recursiveExpressionProvider.apply(iTable), 
				this.k,
				monitor);
		
		return exp;
	}

	@Override
	public TabularExpression buildPostprocessQuery(Table iTable) {
		return LazyExpression.inMemoryRealizer(this.postprocessProvider.apply(iTable));
	}

}
