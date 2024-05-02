package queries;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

import annotations.CallingArg;
import annotations.QueryParameter;
import annotations.QueryParameterGetter;
import data.Electricity;
import rq.common.algorithms.LazyRecursive;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.onOperators.Constant;
import rq.common.onOperators.DivDouble;
import rq.common.onOperators.OnSimilar;
import rq.common.onOperators.PlusDateTime;
import rq.common.onOperators.PlusInteger;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazySelection;
import rq.common.operators.Projection;
import rq.common.restrictions.GreaterThanOrEquals;
import rq.common.restrictions.Or;
import rq.common.restrictions.Similar;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.LazyFacade;
import rq.common.tools.AlgorithmMonitor;

@CallingArg("electricityNoCustTra")
public class Queries2_Electricity_NoCust_tra extends Queries2 {
	
	private  Duration _step = Duration.ofDays(365);
	
	@QueryParameter("step")
	public void setStep(String timeStep) {
		this._step = Duration.ofDays(Integer.parseInt(timeStep));
	}
	
	@QueryParameterGetter("step")
	public String getStep() {
		return this._step.toString();
	}
	
	private  Duration _stepSimilarity = Duration.ofDays(30);
	private BiFunction<Object, Object, Double> stepSimilarity =
			LinearSimilarity.dateTimeSimilarityUntil(_stepSimilarity.toSeconds());
	
	@QueryParameter("stepSimilarity")
	public void setStepSimilarity(String similarityScale) {
		this._stepSimilarity = Duration.ofDays(Integer.parseInt(similarityScale));
		this.stepSimilarity =
				LinearSimilarity.dateTimeSimilarityUntil(_stepSimilarity.toSeconds());
	}
	
	@QueryParameterGetter("stepSimilarity")
	public String getStepSimilarity() {
		return this._stepSimilarity.toString();
	}
	
	private Double _threshold = 1.45d;
	
	@QueryParameter("threshold")
	public void setThreshold(String peakMultiplier) {
		this._threshold = Double.parseDouble(peakMultiplier);
	}
	
	@QueryParameterGetter("threshold")
	public String getThreshold() {
		return Double.toString(this._threshold);
	}
	
	private Double _thresholdSimilarity = 0.2d; 
	private BiFunction<Object, Object, Double> thresholdSimilarity = 
			LinearSimilarity.doubleSimilarityUntil(_thresholdSimilarity);
	
	@QueryParameter("thresholdSimilarity")
	public void setThresholdSimilarity(String peakSimilarity) {
		this._thresholdSimilarity = Double.parseDouble(peakSimilarity);
		this.thresholdSimilarity = LinearSimilarity.doubleSimilarityUntil(_thresholdSimilarity);
	}
	
	@QueryParameterGetter("thresholdSimilarity")
	public String peakSimilarity() {
		return Double.toString(this._thresholdSimilarity);
	}
	
	private int _peaks = 3;
	
	@QueryParameter("peaks")
	public void setPeaks(String peaks) {
		this._peaks = Integer.parseInt(peaks);
	}
	
	@QueryParameterGetter("peaks")
	public String getPeaks() {
		return Integer.toString(this._peaks);
	}
	
	private int _peaksSimilarity = 2;
	private BiFunction<Object, Object, Double> peaksSimilarity = 
			LinearSimilarity.integerSimilarityUntil(_peaksSimilarity);
	
	@QueryParameter("peaksSimilarity")
	public void setPeaksSimilarity(String numberOfPeaksSimilarity) {
		this._peaksSimilarity = Integer.parseInt(numberOfPeaksSimilarity);
		this.peaksSimilarity = LinearSimilarity.integerSimilarityUntil(this._peaksSimilarity);
	}
	@QueryParameterGetter("peaksSimilarity")
	public String getPeaksSimilarity() {
		return Integer.toString(_peaksSimilarity);
	}

	public Queries2_Electricity_NoCust_tra(Class<? extends LazyRecursive> algorithm, AlgorithmMonitor monitor) {
		super(algorithm, monitor);
	}

	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("Electricity")
				.append("NoCust")
				.append("Tra")
				.toString();
	}

	@Override
	protected Function<Table, LazyExpression> initialProvider() throws Exception {
		return (Table iTable) -> {
			return LazyProjection.factory(
					new LazyFacade(iTable),
					new Projection.To(Electricity.time, Electricity.fromTime),
					new Projection.To(Electricity.time, Electricity.toTime),
					new Projection.To(new Constant<Integer>(1), Electricity.peaks));
		};
	}

	@Override
	protected Function<Table, Function<Table, LazyExpression>> recursiveStepProvider() throws Exception {
		return (Table iTable) -> {
			return (Table t) -> {
				return LazyProjection.factory(
						LazyJoin.factory(
								new LazyFacade(t), 
								new LazyFacade(iTable), 
								new OnSimilar(
										new PlusDateTime(Electricity.toTime, _step), 
										Electricity.time, 
										this.stepSimilarity)), 
						new Projection.To(Electricity.fromTime, Electricity.fromTime),
						new Projection.To(Electricity.time, Electricity.toTime),
						new Projection.To(new PlusInteger(Electricity.peaks, new Constant<Integer>(1)), Electricity.peaks));
			};
		};
	}

	@Override
	protected Function<Table, LazyExpression> postprocessProvider() throws Exception {
		return (Table iTable) -> {			
			return new LazySelection(
					new LazyFacade(iTable),
					new Or(	new Similar(
								Electricity.peaks, 
								new Constant<Integer>(this._peaks), 
								this.peaksSimilarity),
							new GreaterThanOrEquals(
									Electricity.peaks, 
									new Constant<Integer>(this._peaks))));
		};
	}

	@Override
	public LazyExpression preprocess(LazyExpression iTable) {
		LazyExpression le =
				new LazySelection(
						iTable,
						new Or(	new Similar(
									new DivDouble(Electricity.value, Electricity.movingAvg), 
									new Constant<Double>(this._threshold), 
									this.thresholdSimilarity),
								new GreaterThanOrEquals(
										new DivDouble(Electricity.value, Electricity.movingAvg), 
										new Constant<Double>(this._threshold))));
		
		return le;
	}
}
