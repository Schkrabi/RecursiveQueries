package rq.common.estimations;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import rq.common.statistic.RankHistogram;

public class EstimateProjectionNominal {

	private final RankHistogram hist;
	private final List<Integer> valueCounts;
	
	public EstimateProjectionNominal(
			RankHistogram hist,
			Collection<Integer> valueCounts) {
		this.hist = hist;
		this.valueCounts = new ArrayList<Integer>(valueCounts);
	}
	
	private BigDecimal estimateOverallSize() {
		BigDecimal agg = BigDecimal.valueOf(1);
		for(var i : this.valueCounts) {
			agg = agg.multiply(BigDecimal.valueOf(i));
		}
		
		return agg.min(BigDecimal.valueOf((long)this.hist.tableSize() / 2));
	}

	public RankHistogram doEstimate() {
		var tableSize = this.hist.tableSize();
		var estSize = this.estimateOverallSize();
		var ratio = estSize.divide(BigDecimal.valueOf(tableSize), 5, RoundingMode.HALF_EVEN).doubleValue();
		
		var r1 = RankHistogram.mult(this.hist, ratio);
		var r2 = RankHistogram.mult(this.hist, 1 - (ratio));
		var est = EstimateUnion.estimate(r1, r2);
		
		var ratio2 = estSize.divide(BigDecimal.valueOf(est.tableSize()), 5, RoundingMode.HALF_EVEN).doubleValue();
		
		est = RankHistogram.mult(est, ratio2);
		return est;
	}
	
	public static RankHistogram estimate(RankHistogram hist, Collection<Integer> valueCounts) {
		var me = new EstimateProjectionNominal(hist, valueCounts);
		var rslt = me.doEstimate();
		return rslt;
	}
}
