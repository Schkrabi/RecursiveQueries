package rq.common.statistic;

import rq.common.interfaces.Table;

/**
 * Subclasses ought to implement various statistics for Tables
 */
public abstract class AbstractStatistic {
	
	/**
	 * Updates the statistic from the table
	 * @param table table the statistic is gathered from
	 */
	public abstract void gather(Table table);
}
