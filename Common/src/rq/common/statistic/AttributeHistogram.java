package rq.common.statistic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.types.Str50;
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
	
	private AttributeHistogram(Attribute attribute, LinkedHashMap<Str50, Integer> data) {
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
		var data = new LinkedHashMap<Str50, Integer>();
		for(var line : serialized.split("\n")) {
			if(attribute == null) {
				attribute = Attribute.parse(line);
				continue;
			}
			var vls = line.split(";");
			data.put(Str50.factory(vls[0]), Integer.parseInt(vls[1]));
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
