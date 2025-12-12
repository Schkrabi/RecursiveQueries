package rq.common.statistic;

import rq.common.interfaces.Table;

public interface IStatistic {

	/**
	 * Updates the statistic from the table
	 * @param table table the statistic is gathered from
	 */
	void gather(Table table);

}