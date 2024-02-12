package rq.common.statistic;

import java.util.HashMap;
import java.util.Map;
import rq.common.table.Attribute;
import rq.common.table.Record;

import rq.common.interfaces.Table;

/**
 * Histogram of values of an attribute
 */
public class AttributeHistogram extends AbstractStatistic {
	
	private final Map<Object, Integer> counts = new HashMap<Object, Integer>();
	public final Attribute counted;
	
	public AttributeHistogram(Attribute attribute) {
		this.counted = attribute;
	}

	@Override
	public void gather(Table table) {
		this.counts.clear();
		
		for(Record r : table) {
			Object value = r.getNoThrow(this.counted);
			if(value != null) {
				Integer count = this.counts.get(value);
				if(count == null) {
					count = Integer.valueOf(1);
				}
				else {
					count++;
				}
				this.counts.put(value, count);
			}
		}
	}

	/**
	 * Gets copy of this value count map
	 * @return new Map<Object, Integer> instance
	 */
	public Map<Object, Integer> getHistogram(){
		return new HashMap<Object,Integer>(this.counts);
	}
	
	/**
	 * Gets the count of specific object in table
	 * @param value counted value
	 * @return Number of occurrences of value
	 */
	public int getCount(Object value) {
		Integer count = this.counts.get(value);
		if(count == null) {
			return 0;
		}
		return count;
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("AttributeHistogram(")
				.append(this.counted.toString())
				.append("):")
				.append(this.counts.toString())
				.toString();
				
	}
}
