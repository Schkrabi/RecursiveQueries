package rq.common.estimations;

import java.util.Map;
import java.util.function.BinaryOperator;

import rq.common.statistic.RankHistogram;
import rq.common.statistic.SlicedStatistic.RankInterval;

public class PostProcessEstimateWithFuzzyTable {

	public final RankHistogram originalEstimate;
	public final RankHistogram dataRankHistogram;
	public final BinaryOperator<Double> product;
	
	public PostProcessEstimateWithFuzzyTable(
			RankHistogram originalEstimate,
			RankHistogram dataRankHistogram,
			BinaryOperator<Double> product) {		
		this.originalEstimate = originalEstimate;
		this.dataRankHistogram = dataRankHistogram;
		this.product = product;
	}
	
	public RankHistogram  doRecalculate() {
		RankHistogram rslt = new RankHistogram(this.dataRankHistogram.getSlices().size());
		
		double estSize = this.originalEstimate.tableSize();
		double dataSize = this.dataRankHistogram.tableSize();
		
		for(RankInterval est : this.originalEstimate.getSlices()) {
			for(RankInterval data : this.dataRankHistogram.getSlices()) {
				RankInterval recalculated = new RankInterval(
						this.product.apply(est.start, data.start),
						this.product.apply(est.end, data.end));
				double estExpected = this.originalEstimate.get(est) / estSize;
				double dataExpected = this.dataRankHistogram.get(data) / dataSize;
				double recalculatedValue = estExpected * dataExpected * dataSize;
				
				Map<RankInterval, Double> splitRatios = 
						recalculated.ratiosOverMultipleIntervals(this.originalEstimate.getSlices());
				for(RankInterval ri : splitRatios.keySet()) {
					rslt.addIntervalValue(ri, recalculatedValue * splitRatios.get(ri));
				}
			}
		}
		return rslt;
	}
	
	/**
	 * To estimate that assumed unranked table adds ranks based on the rank histogram of original table
	 * @param originalEstimate estimate based on unranked table
	 * @param dataRankHistogram rank histogram of original table
	 * @param product 
	 * @return RankHistogram instance
	 */
	public static RankHistogram recalculate(
			RankHistogram originalEstimate,
			RankHistogram dataRankHistogram,
			BinaryOperator<Double> product) {
		PostProcessEstimateWithFuzzyTable me = 
				new PostProcessEstimateWithFuzzyTable(originalEstimate, dataRankHistogram, product);
		
		return me.doRecalculate();
	}

}
