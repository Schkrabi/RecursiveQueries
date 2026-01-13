package rq.estimations.framework;

import rq.common.statistic.RankHistogram;

public abstract class EstimationProvider {

	public EstimationProvider() {
	}

	public abstract RankHistogram estimate();
	public abstract RankHistogram compute();
	
	public abstract String name();
	public abstract EstimationSetupContract contract();
	public abstract int dataSize();
}
