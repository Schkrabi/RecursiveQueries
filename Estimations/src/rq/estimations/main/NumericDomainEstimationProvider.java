package rq.estimations.main;

import rq.common.onOperators.Constant;
import rq.common.operators.Selection;
import rq.common.restrictions.Similar;
import rq.common.statistic.RankHistogram;

public abstract class NumericDomainEstimationProvider extends EstimationProvider {

	private final UnaryOperationContract contract;
	
	public NumericDomainEstimationProvider(UnaryOperationContract contract) {
		this.contract = contract;
	}

	@Override
	public RankHistogram compute() {
		var selection = new Selection(
				this.contract.getTable(),
				new Similar(
						this.contract.getAttribute(),
						new Constant<Double>(this.contract.getValue()),
						this.contract.getSimilarity()));
		
		var rslt = selection.eval();
		var stats = rslt.getStatistics();
		stats.addRankHistogram(this.contract.getSlices());
		stats.gather();
		
		var hist = stats.getRankHistogram(this.contract.getSlices()).get();
		
		return hist;
	}

	@Override
	public EstimationSetupContract contract() {
		return this.contract;
	}

	@Override
	public int dataSize() {
		return this.contract.getTable().size();
	}

}
