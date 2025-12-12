package rq.common.statistic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.interfaces.Table;
import rq.common.io.contexts.ClassNotInContextException;
import rq.common.io.contexts.ValueParserContext;

/**
 * Histogram of values of an attribute
 */
public class AttributeHistogram implements IStatistic {
	
	private final Map<Object, Integer> counts = new HashMap<Object, Integer>();
	public final Attribute counted;
	
	public AttributeHistogram(Attribute attribute) {
		this.counted = attribute;
	}
	
	private AttributeHistogram(Attribute attribute, LinkedHashMap<Object, Integer> data) {
		this.counted = attribute;
		this.counts.putAll(data);
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
	
	public String serialize() {
		var sb = new StringBuilder();
		
		sb.append(this.counted.serialize())
			.append("\n");
		
		for(var e : this.counts.entrySet()) {
			sb.append(e.getKey().toString())
				.append(";")
				.append(e.getValue())
				.append("\n");
		}
		
		return sb.toString();
	}
	
	public static AttributeHistogram deserialize(String serialized) throws ClassNotFoundException {
		Attribute attribute = null;
		var data = new LinkedHashMap<Object, Integer>();
		for(var line : serialized.split("\n")) {
			if(attribute == null) {
				attribute = Attribute.parse(line);
				continue;
			}
			var vls = line.split(";");
			try {
				data.put(ValueParserContext.DEFAULT.parseValue(attribute.domain(), vls[0]), Integer.parseInt(vls[1]));
			} catch (NumberFormatException | ClassNotInContextException e) {
				throw new RuntimeException(e);
			}
		}
		return new AttributeHistogram(attribute, data);
	}
	
	public void writeFile(String path) throws IOException {
		Files.write(Path.of(path), this.serialize().getBytes());
	}
	
	public void writeFile(Path path) throws IOException {
		Files.write(path, this.serialize().getBytes());
	}
	
	public static AttributeHistogram readFile(String path) throws ClassNotFoundException, IOException {
		return readFile(Path.of(path));
	}
	
	public static AttributeHistogram readFile(Path path) throws ClassNotFoundException, IOException {
		return deserialize(Files.readString(path));
	}
}
