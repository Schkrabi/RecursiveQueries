package rq.common.interfaces;

public interface LazyIterator {
	
	public rq.common.table.Record next();
	
	/**
	 * Returns last returned value from the sequence. If none element was retrieved yet, returns null
	 * @return T or null
	 */
	public rq.common.table.Record current();
	
	/**
	 * Restarts the iterator from the top
	 */
	public void restart();
}
