package queries;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

import annotations.CallingArg;
import annotations.QueryParameter;
import annotations.QueryParameterGetter;
import data.Retail;
import rq.common.algorithms.LazyRecursive;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.onOperators.Constant;
import rq.common.onOperators.DivDouble;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnLesserThan;
import rq.common.onOperators.OnSimilar;
import rq.common.onOperators.PlusDateTime;
import rq.common.onOperators.PlusInteger;
import rq.common.operators.Join;
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

@CallingArg("retailTop")
public class Queries2_Retail_top extends Queries2 {
	
	private Double _threshold = 2.5d;
	private Double _thresholdSimilarity = 1.d;
	private BiFunction<Object, Object, Double> thresholdSimilarity =
			LinearSimilarity.doubleSimilarityUntil(_thresholdSimilarity);
	
	@QueryParameter("threshold")
	public void setTreshold(String threshold) {
		this._threshold = Double.parseDouble(threshold);
	}
	
	@QueryParameter("thresholdSimilarity")
	public void setThresholdSimilarity(String thresholdSimilarity) {
		this._thresholdSimilarity = Double.parseDouble(thresholdSimilarity);
		this.thresholdSimilarity = LinearSimilarity.doubleSimilarityUntil(this._thresholdSimilarity);
	}
	
	@QueryParameterGetter("threshold")
	public String getThreshold() {
		return Double.toString(this._threshold);
	}
	
	@QueryParameterGetter("thresholdSimilarity")
	public String getThresholdSimilarity() {
		return Double.toString(this._thresholdSimilarity);
	}
	
	private Duration _step = Duration.ofDays(365);
	
	@QueryParameter("step")
	public void setStep(String step) {
		this._step = Duration.ofDays(Integer.parseInt(step));
	}
	
	@QueryParameterGetter("step")
	public String getStep() {
		return this._step.toString();
	}

	private Duration _stepSimilarity = Duration.ofDays(14);
	private BiFunction<Object, Object, Double> stepSimilarity = 
			LinearSimilarity.dateTimeSimilarityUntil(_stepSimilarity.toSeconds());
	
	@QueryParameter("stepSimilarity")
	public void setSimilarityScale(String similarityScale) {
		this._stepSimilarity = Duration.ofDays(Integer.parseInt(similarityScale));
		this.stepSimilarity = 
				LinearSimilarity.dateTimeSimilarityUntil(_stepSimilarity.toSeconds());
	}
	
	@QueryParameterGetter("stepSimilarity")
	public String getSimilarityScale() {
		return this._stepSimilarity.toString();
	}

	public Queries2_Retail_top(Class<? extends LazyRecursive> algorithm, AlgorithmMonitor monitor) {
		super(algorithm, monitor);
	}

	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("Retail")
				.append("Top")
				.toString();
	}

	@Override
	protected Function<Table, LazyExpression> initialProvider() throws Exception {
		return (Table iTable) -> {
			return LazyProjection.factory(
					new LazyFacade(iTable), 
					new Projection.To(Retail.invoiceDate, Retail.fromTime),
					new Projection.To(Retail.invoiceDate, Retail.toTime),
					new Projection.To(new Constant<Integer>(1), Retail.peaks),
					new Projection.To(Retail.stockCode, Retail.stockCode));
		};
	}

	@Override
	protected Function<Table, Function<Table, LazyExpression>> recursiveStepProvider() throws Exception {
		return (Table iTable) -> {
			return (Table t) ->{
				return
				LazyProjection.factory(
						LazyJoin.factory(
								new LazyFacade(t), 
								new LazyFacade(iTable), 
								new OnEquals(Retail.stockCode, Retail.stockCode),
								new OnLesserThan(Retail.toTime, Retail.invoiceDate),
								new OnSimilar(new PlusDateTime(Retail.toTime, _step), 
										Retail.invoiceDate,
										this.stepSimilarity)), 
						new Projection.To(Join.left(Retail.stockCode), Retail.stockCode),
						new Projection.To(Retail.fromTime, Retail.fromTime),
						new Projection.To(Retail.invoiceDate, Retail.toTime),
						new Projection.To(new PlusInteger(Retail.peaks, new Constant<Integer>(1)), Retail.peaks));
			};
		};
	}

	@Override
	protected Function<Table, LazyExpression> postprocessProvider() throws Exception {
		return (Table iTable) -> new LazyFacade(iTable);
	}

	@Override
	public LazyExpression preprocess(LazyExpression iTable) {
		LazyExpression le = null;
		
		le = new LazySelection(
				iTable,
				new Or(	new Similar(
							new DivDouble(Retail.quantity, Retail.qtyMovAvg), 
							new Constant<Double>(this._threshold), 
							this.thresholdSimilarity),
						new GreaterThanOrEquals(
								new DivDouble(Retail.quantity, Retail.qtyMovAvg), 
								new Constant<Double>(this._threshold))));
		
		return le;
	}

}
