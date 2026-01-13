package rq.common.statistic;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.interfaces.Table;

/**
 * Histogram of values of an attribute
 * @param <T>
 */
public class AttributeHistogram<T> implements IStatistic {
	
	private final Map<T, Integer> counts = new HashMap<T, Integer>();
	public final Attribute<T> counted;
	
	public AttributeHistogram(Attribute<T> attribute) {
		this.counted = attribute;
	}
	
//	private AttributeHistogram(Attribute<T> attribute, LinkedHashMap<T, Integer> data) {
//		this.counted = attribute;
//		this.counts.putAll(data);
//	}

	@Override
	public void gather(Table table) {
		this.counts.clear();
		
		for(Record r : table) {
			T value = (T)r.getNoThrow(this.counted);
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
	public Map<T, Integer> getHistogram(){
		return new HashMap<T,Integer>(this.counts);
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
	
	/** Return number of distinct values in the histogram */
	public int distinctValues() {
		return this.counts.size();
	}
	
	private List<Object> _vlsByFrq = null;
	
	/** Gets the values of the attribute sorted by frequency of occurrence, descending */
	public List<Object> valuesByFrequency(){
		if(_vlsByFrq == null) {
			var l = new ArrayList<>(this.counts.entrySet());
			l.sort(Map.Entry.comparingByValue((x1, x2) -> -Integer.compare((Integer)x1, (Integer)x2)));
			_vlsByFrq = l.stream().map(e->e.getKey()).collect(Collectors.toList());
		}
		return _vlsByFrq;
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
