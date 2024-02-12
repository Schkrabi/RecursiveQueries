/**
 * 
 */
package rq.common.statistic;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import rq.common.interfaces.Table;
import rq.common.table.Attribute;
import rq.common.table.Record;

/**
 * Value Count of attributes of a table
 */
public class ValueCount extends AbstractStatistic {

	private Map<Attribute, Integer> valueCounts = new HashMap<Attribute, Integer>();

	@Override
	public void gather(Table table) {
		Map<Attribute, Integer> m = new HashMap<Attribute, Integer>();
		for(Attribute a : table.schema()) {
			Set<Object> values = new HashSet<Object>();
			for(Record r : table) {
				Object v = r.getNoThrow(a);
				values.add(v);
			}
			m.put(a, values.size());
		}
		this.valueCounts = m;
	}
	
	/**
	 * Gets value count (number of distinct values) in a table attribute
	 * @param attribute inspected attribute
	 * @return value count of the attribute
	 */
	public int getValueCount(Attribute attribute) {
		Integer c = this.valueCounts.get(attribute);
		if(c == null) {
			return 0;
		}
		return c;
	}

	@Override
	public String toString() {
		return new StringBuilder()
				.append("ValueCounts:")
				.append(this.valueCounts.toString())
				.toString();
	}
}
