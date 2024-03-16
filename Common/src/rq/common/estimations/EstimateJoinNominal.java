package rq.common.estimations;

import java.util.function.BinaryOperator;

import rq.common.statistic.AttributeHistogram;
import rq.common.statistic.RankHistogram;

public class EstimateJoinNominal {

	private final RankHistogram left;
	private final RankHistogram right;
	private final AttributeHistogram leftAttributeHistogram;
	private final AttributeHistogram rightAttributeHistogram;
	private final BinaryOperator<Double> product;
	
	public EstimateJoinNominal(
			RankHistogram left,
			RankHistogram right,
			AttributeHistogram leftAttributeHistogram,
			AttributeHistogram rightAttributeHistogram,
			BinaryOperator<Double> product) {
		this.left = left;
		this.right = right;
		this.leftAttributeHistogram = leftAttributeHistogram;
		this.rightAttributeHistogram = rightAttributeHistogram;
		this.product = product;
	}

	public RankHistogram doEstimate() {
		//Estimate the crossjoin
		var crossJoinHist = EstimateCrossJoin.estimate(left, right, this.product);
		
		//Estimate the size of the Join

		var joinSize = this.joinSize();
		var ratio = joinSize / crossJoinHist.tableSize();
		
		return RankHistogram.mult(crossJoinHist, ratio);
	}
	
	private double joinSize() {
//		var sum = 0;
//		for(var value : this.leftAttributeHistogram.getHistogram().keySet()) {
//			var lCount = this.leftAttributeHistogram.getCount(value);
//			var rCount = this.rightAttributeHistogram.getCount(value);
//			
//			sum += lCount * rCount;
//		}
//		return sum;
		var leftValueCount = this.leftAttributeHistogram.distinctValues();
		var rightValueCount = this.rightAttributeHistogram.distinctValues();
		
		return (this.left.tableSize() * this.right.tableSize()) / Math.max(leftValueCount, rightValueCount);		
	}
	
	public static RankHistogram estimate(
			RankHistogram left,
			RankHistogram right,
			AttributeHistogram leftAttributeHistogram,
			AttributeHistogram rightAttributeHistogram,
			BinaryOperator<Double> product) {
		var me = new EstimateJoinNominal(left, right, leftAttributeHistogram, rightAttributeHistogram, product);
		var est = me.doEstimate();
		return est;
	}
}
