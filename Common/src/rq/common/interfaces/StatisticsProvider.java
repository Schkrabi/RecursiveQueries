package rq.common.interfaces;

import rq.common.statistic.Statistics;

/**
 * Interface providing a table statistics
 */
public interface StatisticsProvider {
	public Statistics getStatistics();
	public boolean hasStatistics();
}
