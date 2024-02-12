package rq.common.statistic;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalDouble;

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
}
