package rq.common.statistic;

import java.util.HashMap;
import java.util.Map;

import rq.common.interfaces.Table;
import rq.common.table.Attribute;

/**
 * Sliced histogram statistic
 */
public class SlicedHistogram extends SlicedStatistic {
	
	/**
	 * Monitored attribute
	 */
	public final Attribute attribute;
	
	final Map<RankInterval, Integer> rankHistogram = new HashMap<RankInterval, Integer>();
	private final Map<RankInterval, Map<Object, Integer>> histograms = new HashMap<RankInterval, Map<Object, Integer>>();
	
	public SlicedHistogram(Attribute attribute, int slices) {
		super(uniformSlices(slices));
		this.attribute = attribute;
		this.initHistograms();
	}
	
	/**
	 * Gets histogram for specific slice
	 * @param interval slice
	 * @return histogram map
	 */
	public Map<Object, Integer> histogramSlice(RankInterval interval){
		Map<Object, Integer> histogram = this.histograms.get(interval);
		if(histogram == null) {
			return null;
		}
		
		return new HashMap<Object, Integer>(histogram);
	}
	
	/**
	 * Gets histogram for specific slice
	 * @param start start of the slice
	 * @param end end of the slice
	 * @return histogram map
	 */
	public Map<Object, Integer> histogramSlice(double start, double end){
		return this.histogramSlice(new RankInterval(start, end));
	}
	
	/**
	 * Gets count of specific value in specific slice
	 * @param interval slice interval
	 * @param value inspected value
	 * @return count of the value in the slice
	 */
	public int getCount(RankInterval interval, Object value) {
		Map<Object, Integer> slice = this.histogramSlice(interval);
		
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
		for(RankInterval interval : this.slices) {
			Map<Object, Integer> histogram = new HashMap<Object, Integer>();
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
			
			Map<Object, Integer> histogram = this.histograms.get(interval);
			if(histogram == null) {
				histogram = new HashMap<Object, Integer>();
				this.histograms.put(interval, histogram);
			}
			
			Object value = r.getNoThrow(attribute);
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
