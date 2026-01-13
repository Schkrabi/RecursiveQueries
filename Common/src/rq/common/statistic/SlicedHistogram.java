package rq.common.statistic;

import java.util.HashMap;
import java.util.Map;

import rq.common.interfaces.Table;
import rq.common.table.Attribute;

/**
 * Sliced histogram statistic
 */
public class SlicedHistogram<T> extends SlicedStatistic {
	
	/**
	 * Monitored attribute
	 */
	public final Attribute<T> attribute;
	
	final Map<RankInterval, Integer> rankHistogram = new HashMap<>();
	private final Map<RankInterval, Map<T, Integer>> histograms = new HashMap<>();
	
	public SlicedHistogram(Attribute<T> attribute, int slices) {
		super(uniformSlices(slices));
		this.attribute = attribute;
		this.initHistograms();
	}
	
	/**
	 * Gets histogram for specific slice
	 * @param interval slice
	 * @return histogram map
	 */
	public Map<T, Integer> histogramSlice(RankInterval interval){
		var histogram = this.histograms.get(interval);
		if(histogram == null) {
			return null;
		}
		
		return new HashMap<>(histogram);
	}
	
	/**
	 * Gets histogram for specific slice
	 * @param start start of the slice
	 * @param end end of the slice
	 * @return histogram map
	 */
	public Map<T, Integer> histogramSlice(double start, double end){
		return this.histogramSlice(new RankInterval(start, end));
	}
	
	/**
	 * Gets count of specific value in specific slice
	 * @param interval slice interval
	 * @param value inspected value
	 * @return count of the value in the slice
	 */
	public int getCount(RankInterval interval, Object value) {
		var slice = this.histogramSlice(interval);
		
		if(slice == null) {
			return 0;
		}
		
		Integer count = slice.get(value);; 
		
		if(count == null) {
			return 0;
		}
		return count;
	}
	
	/**
	 * Gets count of specific value in specific slice
	 * @param start start of the slice
	 * @param end end of the slice
	 * @param value inspected value
	 * @return count of the value in the slice
	 */
	public int getCount(double start, double end, Object value) {
		return this.getCount(new RankInterval(start, end), value);
	}
	
	/**
	 * Initializes inner histogram structure
	 */
	private void initHistograms() {
		this.histograms.clear();
		for(var interval : this.slices) {
			var histogram = new HashMap<T, Integer>();
			this.histograms.put(interval, histogram);
		}
	}

	@Override
	public void gather(Table table) {
		this.rankHistogram.clear();
		this.initHistograms();
		
		for(rq.common.table.Record r : table) {
			RankInterval interval = this.slices.stream()
					.filter(x -> x.isMember(r.rank))
					.findAny().get();
			
			Integer count = this.rankHistogram.get(interval);
			if(count == null) {
				this.rankHistogram.put(interval, 1);
			} else {
				this.rankHistogram.put(interval, count + 1);
			}
			
			var histogram = this.histograms.get(interval);
			if(histogram == null) {
				histogram = new HashMap<>();
				this.histograms.put(interval, histogram);
			}
			
			var value = r.getNoThrow(attribute);
			Integer hCount = histogram.get(value);
			if(hCount == null) {
				histogram.put(value, 1);
			} else {
				histogram.put(value, hCount + 1);
			}
		}
	}
	
	/**
	 * Gets the size of specified slice or 0 if such slice is not gathered
	 * @param interval searched interval
	 * @return size of the slice or 0
	 */
	public int sliceSize(SlicedStatistic.RankInterval interval) {
		Integer size = this.rankHistogram.get(interval);
		if(size == null) {
			return 0;
		}
		return size;
	}
	
	/**
	 * Gets the size of specified slice or 0 if such slice is not gathered
	 * @param start start of searched interval, exclusive
	 * @param end end of searched interval, inclusive
	 * @return size of the slice or 0
	 */
	public int sliceSize(double start, double end) {
		return this.sliceSize(new SlicedStatistic.RankInterval(start, end));
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("Shistogram(")
				.append(this.attribute.toString())
				.append("): ")
				.append(this.histograms.toString())
				.toString();
	}
}
