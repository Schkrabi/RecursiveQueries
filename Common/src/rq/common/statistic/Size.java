package rq.common.statistic;

import rq.common.interfaces.Table;

public class Size implements IStatistic {

	private int size = 0;

	@Override
	public void gather(Table table) {
		this.size = table.size();
	}

	public int getSize() {
		return this.size;
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("Size:")
				.append(this.size)
				.toString();
	}
}
