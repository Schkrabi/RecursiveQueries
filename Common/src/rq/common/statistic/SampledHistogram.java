package rq.common.statistic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;

import rq.common.interfaces.Table;
import rq.common.table.Attribute;

public class SampledHistogram extends AbstractStatistic {

	public final double sampleSize;
	public final Attribute observed;
	
	private final Map<Double, Integer> data = new HashMap<Double, Integer>();
	
	public SampledHistogram(
			Attribute observed,
			double sampleSize) {
		this.observed = observed;
		this.sampleSize = sampleSize;
	}
	
	private SampledHistogram(
			Attribute observed,
			double sampleSize,
			Map<Double, Integer> data) {
		this.data.putAll(data);
		this.observed = observed;
		this.sampleSize = sampleSize;
	}

	@Override
	public void gather(Table table) {
		this.data.clear();

		for(rq.common.table.Record record : table) {
			double value = (Double)record.getNoThrow(this.observed);
			double sample = this.sample(value);
			Integer count = this.data.get(sample);
			if(count == null) {
				this.data.put(sample, 1);
			}
			else {
				this.data.put(sample, count + 1);
			}
		}
	}

	protected double sample(double value) {
		long mult = Math.round(value / this.sampleSize);
		
		return mult * this.sampleSize;
	}
	
	public Map<Double, Integer> getHistogram(){
		return new HashMap<Double, Integer>(this.data);
	}
	
	public int getCount(double value) {
		double sample = this.sample(value);
		Integer count = this.data.get(sample);
		if(count == null) {
			return 0;
		}
		return count;
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("Sampled Histogram (")
				.append(this.sampleSize)
				.append("): ")
				.append(this.data.toString())
				.toString();
	}
	
	public double max() {
		OptionalDouble max = this.data.keySet().stream().mapToDouble(x -> x).max();
		if(max != null) {
			return max.getAsDouble();
		}
		return 0.d;
	}
	
	public double min() {
		OptionalDouble min = this.data.keySet().stream().mapToDouble(x -> x).min();
		if(min != null) {
			return min.getAsDouble();
		}
		return 0.d;
	}
	
	/**
	 * Gets the values truly observed by the histogram
	 * @return
	 */
	public Set<Double> values(){
		return this.data.keySet();
	}
	
	public String serialize() {
		var sb = new StringBuilder();
		
		sb.append(this.observed.serialize()).append("\n")
			.append(this.sampleSize).append("\n");
		
		for(var e : this.data.entrySet()) {
			sb.append(e.getKey()).append(";")
				.append(e.getValue()).append("\n");
		}
		
		return sb.toString();
	}
	
	public static SampledHistogram deserialize(String serialized) throws ClassNotFoundException {
		var data = new LinkedHashMap<Double, Integer>();
		
		Attribute observed = null;
		Double sampleSize = null;
		
		for(var line : serialized.split("\n")) {
			if(observed == null) {
				observed = Attribute.parse(line);
				continue;
			}
			if(sampleSize == null) {
				sampleSize = Double.parseDouble(line);
				continue;
			}
			
			var vls = line.split(";");
			var key = Double.parseDouble(vls[0]);
			var value = Integer.parseInt(vls[1]);
			data.put(key, value);
		}
		
		return new SampledHistogram(observed, sampleSize, data);
	}
	
	public void writeFile(String path) throws IOException {
		Files.write(Path.of(path), this.serialize().getBytes());
	}
	
	public void writeFile(Path path) throws IOException {
		Files.write(path, this.serialize().getBytes());
	}
	
	public static SampledHistogram readFile(String path) throws IOException, ClassNotFoundException {
		return readFile(Path.of(path));
	}
	
	public static SampledHistogram readFile(Path path) throws IOException, ClassNotFoundException {
		return deserialize(Files.readString(path));
	}
}
