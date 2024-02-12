package rq.estimations.main;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import rq.common.statistic.RankHistogram;
import rq.common.statistic.SlicedStatistic.RankInterval;
import rq.common.table.Attribute;
import rq.common.interfaces.Table;
import rq.common.onOperators.Constant;
import rq.common.operators.AbstractSelection;
import rq.common.operators.Selection;
import rq.common.restrictions.Similar;

public class Measurement {
	
	public final Attribute attribute;
	public final double value;
	public final BiFunction<Object, Object, Double> similarity;
	public final Table data;
	public final EstimationSetupContract contract;
	
	public final BiFunction<AbstractSelection, EstimationSetupContract, RankHistogram> estimateProvider;
	
	private final String resultHeader = "average accuracy, attribute, value, estimated data, actual data, contract";

	public Measurement(
			Attribute attribute,
			double value,
			Table data,
			EstimationSetupContract contract,
			BiFunction<Object, Object, Double> similarity,
			BiFunction<AbstractSelection, EstimationSetupContract, RankHistogram> estimateProvider) {
		this.attribute = attribute;
		this.value = value;
		this.data = data;
		this.contract = contract;
		this.similarity = similarity;
		this.estimateProvider = estimateProvider;
	}

	public String measure() {
		Selection selection = new Selection(
								this.data,
								new Similar(this.attribute, new Constant<Double>(value), this.similarity));
		
		RankHistogram estimate = this.estimateProvider.apply(selection, this.contract);
		
		Table selected = selection.eval();
		selected.getStatistics().addRankHistogram(estimate.getSlices());
		selected.getStatistics().gather();
		
		RankHistogram actual = selected.getStatistics().getRankHistogram(estimate.getSlices()).get();
		
		return this.outputResult(estimate, actual);
	}
	
	private String outputResult(
			RankHistogram estimated,
			RankHistogram actual) {
		return new StringBuilder()
				.append(this.resultHeader).append("\n")
				.append(this.averageAccuracy(estimated, actual)).append(",")
				.append(this.attribute.name).append(",")
				.append(this.value).append(",")
				.append("\"").append(estimated.get().toString()).append("\",")
				.append("\"").append(actual.get().toString()).append("\",")
				.append("\"").append(this.contract.toString()).append("\"")
				.toString();
	}
	
	private double accuracy(double estimated, double actual) {
		return Math.abs(actual - estimated);
	}
	
	private Map<RankInterval, Double> accuracy(
			RankHistogram estimated,
			RankHistogram actual){
		Map<RankInterval, Double> rslt = new HashMap<RankInterval, Double>();
		
		for(RankInterval ri : estimated.get().keySet()) {
			double acc = this.accuracy(estimated.get(ri), actual.get(ri));
			rslt.put(ri, acc);
		}
		
		return rslt;
	}
	
	private double averageAccuracy(
			RankHistogram estimated,
			RankHistogram actual) {
		double avg = this.accuracy(estimated, actual).values().stream()
						.mapToDouble(x -> x).average().getAsDouble();
		return avg;
	}
}
