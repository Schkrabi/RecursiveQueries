package rq.common.statistic;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class SlicedStatistic implements IStatistic {

	protected final Set<RankInterval> slices;

	public static Set<RankInterval> uniformSlices(int n) {
		double start = 0.0d;
		double step = 1.0d / (double)n;
		Set<RankInterval> ret = new HashSet<RankInterval>();
		
		while(start < 1.0d) {
			RankInterval i = new RankInterval(start, Math.min(start + step, 1.0d));
			ret.add(i);
			start += step;
		}
		
		return ret;
	}

	/**
	 * Gets slices this histogram is capturing
	 * @return set of sliced rank intervals
	 */
	public Set<RankInterval> getSlices() {
		return new HashSet<RankInterval>(this.slices);
	}

	public static class RankInterval {
		public final double start;
		public final double end;
		
		/**
		 * Creates a RankInterval
		 * @param start start of the interval, exclusive
		 * @param end end of the interval inclusive
		 */
		public RankInterval(double start, double end) {
			if(start < 0.0d) {
				throw new RuntimeException("start must be greater than or equal to 0.0d.");
			}
			if(start >= 1.0d) {
				throw new RuntimeException("start must be lesser than to 1.0d.");
			}
			if(end <= 0.0d) {
				throw new RuntimeException("end must be greater than 0.0d");
			}
			if(end > 1.0d) {
				throw new RuntimeException("end must be lesser than or equal to 1.0d");
			}
			if(end <= start) {
				throw new RuntimeException("end must be greater than start");
			}
			this.start = start;
			this.end = end;
		}
		
		/**
		 * Returns true if given value is member of this interval. Returns false otherwise.
		 * @param value inspected value
		 * @return true or false
		 */
		public boolean isMember(double value) {
			return value > this.start
					&& value <= this.end;
		}
		
		@Override
		public String toString() {
			return new StringBuilder()
					.append("(")
					.append(this.start)
					.append(",")
					.append(this.end)
					.append("]")
					.toString();
		}
		
		@Override
		public int hashCode() {
			return new StringBuilder()
					.append(this.start)
					.append(this.end)
					.toString()
					.hashCode();
		}
		
		@Override
		public boolean equals(Object other) {
			if(other instanceof RankInterval) {
				RankInterval o = (RankInterval)other;
				return this.start == o.start
						&& this.end == o.end;
			}
			return false;
		}
		
		/**
		 * Returns map of the ratios for spliting a value in interval overlapping several intervals 
		 * @param intervals intervals over which interval is split
		 * @return map of interval - ratio
		 */
		public Map<RankInterval, Double> ratiosOverMultipleIntervals(Set<RankInterval> intervals){
			Map<RankInterval, Double> rslt = new HashMap<RankInterval, Double>();
			double len = this.end - this.start;
			
			for(RankInterval i : intervals) {
				double part = Math.max(0.d, Math.min(this.end, i.end) - Math.max(this.start, i.start));
				double ratio = part/len;
				rslt.put(i, ratio);
			}
			
			return rslt;
		}
	}

	public SlicedStatistic(Set<RankInterval> slices) {
		super();
		this.slices = slices;
	}
}