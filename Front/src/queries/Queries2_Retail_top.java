package queries;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

import annotations.CallingArg;
import annotations.QueryParameter;
import annotations.QueryParameterGetter;
import data.Retail;
import rq.common.algorithms.LazyRecursive;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.onOperators.Constant;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnLesserThan;
import rq.common.onOperators.OnSimilar;
import rq.common.onOperators.PlusDateTime;
import rq.common.onOperators.PlusInteger;
import rq.common.operators.Join;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.LazyFacade;
import rq.common.table.Record;
import rq.common.tools.AlgorithmMonitor;

@CallingArg("retailTop")
public class Queries2_Retail_top extends Queries2 {
	
	private Double MAX_QTY_MULT = 2.5d;
	
	@QueryParameter("maxQtyMult")
	public void setMaxQtyMult(String maxQtyMult) {
		this.MAX_QTY_MULT = Double.parseDouble(maxQtyMult);
	}
	
	@QueryParameterGetter("maxQtyMult")
	public String getMaxQtyMult() {
		return Double.toString(this.MAX_QTY_MULT);
	}
	
	private Double MIN_QTY_MULT = 1.5d;
	
	@QueryParameter("minQtyMult")
	public void setMinQtyMult(String mixQtyMult) {
		this.MIN_QTY_MULT = Double.parseDouble(mixQtyMult);
	}
	
	@QueryParameterGetter("minQtyMult")
	public String getMinQtyMult() {
		return Double.toString(this.MIN_QTY_MULT);
	}
	
	private Duration STEP = Duration.ofDays(365);
	
	@QueryParameter("step")
	public void setStep(String step) {
		this.STEP = Duration.ofDays(Integer.parseInt(step));
	}
	
	@QueryParameterGetter("step")
	public String getStep() {
		return this.STEP.toString();
	}

	private Duration SIMILARITY_SCALE = Duration.ofDays(14);
	
	@QueryParameter("similarityScale")
	public void setSimilarityScale(String similarityScale) {
		this.SIMILARITY_SCALE = Duration.ofDays(Integer.parseInt(similarityScale));
	}
	
	@QueryParameterGetter("similarityScale")
	public String getSimilarityScale() {
		return this.SIMILARITY_SCALE.toString();
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
			try {
				return LazyProjection.factory(
						new LazyFacade(iTable), 
						new Projection.To(Retail.invoiceDate, Retail.fromTime),
						new Projection.To(Retail.invoiceDate, Retail.toTime),
						new Projection.To(new Constant<Integer>(1), Retail.peaks),
						new Projection.To(Retail.stockCode, Retail.stockCode));
			} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException e) {
				throw new RuntimeException(e);
			}
		};
	}

	@Override
	protected Function<Table, Function<Table, LazyExpression>> recursiveStepProvider() throws Exception {
		return (Table iTable) -> {
			return (Table t) ->{
				try {
					return
					LazyProjection.factory(
							LazyJoin.factory(
									new LazyFacade(t), 
									new LazyFacade(iTable), 
									new OnEquals(Retail.stockCode, Retail.stockCode),
									new OnLesserThan(Retail.toTime, Retail.invoiceDate),
									new OnSimilar(new PlusDateTime(Retail.toTime, STEP), 
											Retail.invoiceDate,
											LinearSimilarity.dateTimeSimilarityUntil(SIMILARITY_SCALE.toSeconds()))), 
							new Projection.To(Join.left(Retail.stockCode), Retail.stockCode),
							new Projection.To(Retail.fromTime, Retail.fromTime),
							new Projection.To(Retail.invoiceDate, Retail.toTime),
							new Projection.To(new PlusInteger(Retail.peaks, new Constant<Integer>(1)), Retail.peaks));
				} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException
						| OnOperatornNotApplicableToSchemaException e) {
					throw new RuntimeException(e);
				}
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
		
		final BiFunction<Object, Object, Double> ratioSim = LinearSimilarity.doubleSimilarityUntil(MAX_QTY_MULT - MIN_QTY_MULT);
		
		le = LazyRestriction.factory(
				iTable, 
				(Record r) -> {
					int qty = (int) r.getNoThrow(Retail.quantity);
					double qtyMovAvg = (double) r.getNoThrow(Retail.qtyMovAvg);
					if(qty < qtyMovAvg) {
						return 0.0d;
					}
					double ratio = qty/qtyMovAvg;
					if(ratio >= MAX_QTY_MULT) {
						return 1.0d;
					}
					return ratioSim.apply(MAX_QTY_MULT, ratio);
				});
		
		return le;
	}

}
