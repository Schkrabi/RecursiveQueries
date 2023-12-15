package queries;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

import annotations.CallingArg;
import annotations.QueryParameter;
import annotations.QueryParameterGetter;
import data.Electricity;
import rq.common.algorithms.LazyRecursive;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.latices.Goguen;
import rq.common.onOperators.Constant;
import rq.common.onOperators.OnEquals;
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
import rq.common.tools.AlgorithmMonitor;

@CallingArg("electricityWeekFuzzyTra")
public class Queries2_Electricity_Week_Fuzzy_tra extends Queries2 {

	private Duration TIME_STEP = Duration.ofDays(60);
	
	@QueryParameter("timeStep")
	public void setTimeStep(String timeStep) {
		this.TIME_STEP = Duration.ofDays(Integer.parseInt(timeStep));
	}
	
	@QueryParameterGetter("timeStep")
	public String getTimeStep() {
		return this.TIME_STEP.toString();
	}
	
	private Duration SIMILARITY_SCALE = Duration.ofDays(20);
	
	@QueryParameter("similarityScale")
	public void setSimilarityScale(String similarityScale) {
		this.SIMILARITY_SCALE = Duration.ofDays(Integer.parseInt(similarityScale));
	}
	
	@QueryParameterGetter("similarityScale")
	public String getSimilarityScale() {
		return this.SIMILARITY_SCALE.toString();
	}
	
	private Double PEAK_MULTIPLIER = 1.2d;
	
	@QueryParameter("peakMultiplier")
	public void setPeakMultiplier(String peakMultiplier) {
		this.PEAK_MULTIPLIER = Double.parseDouble(peakMultiplier);
	}
	
	@QueryParameterGetter("peakMultiplier")
	public String getPeakMultiplier() {
		return Double.toString(this.PEAK_MULTIPLIER);
	}
	
	private Double PEAK_SIMILARITY = 0.2d;
	
	@QueryParameter("peakSimilarity")
	public void setPeakSimilarity(String peakSimilarity) {
		this.PEAK_SIMILARITY = Double.parseDouble(peakSimilarity);
	}
	
	@QueryParameterGetter("peakSimilarity")
	public String getPeakSimilarity() {
		return Double.toString(this.PEAK_SIMILARITY);
	}
	
	private int SEARCHED_NUMBER_OF_PEAKS = 10;
	
	@QueryParameter("peaks")
	public void setPeaks(String peaks) {
		this.SEARCHED_NUMBER_OF_PEAKS = Integer.parseInt(peaks);
	}
	
	@QueryParameterGetter("peaks")
	public String getPeaks() {
		return Integer.toString(this.SEARCHED_NUMBER_OF_PEAKS);
	}
	
	public Queries2_Electricity_Week_Fuzzy_tra(Class<? extends LazyRecursive> algorithm, AlgorithmMonitor monitor) {
		super(algorithm, monitor);
	}

	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("Electricity")
				.append("Week")
				.append("Fuzzy")
				.append("Tra")
				.toString();
	}

	@Override
	protected Function<Table, LazyExpression> initialProvider() throws Exception {
		return (Table iTable) -> {
			try {
				return LazyProjection.factory(
						new LazyFacade(iTable),  
						new Projection.To(Electricity.customer, Electricity.customer),
						new Projection.To(Electricity.time, Electricity.fromTime),
						new Projection.To(Electricity.time, Electricity.toTime),
						new Projection.To(new Constant<Integer>(1), Electricity.peaks));
			} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException e) {
				throw new RuntimeException(e);
			}
		};
	}

	@Override
	protected Function<Table, Function<Table, LazyExpression>> recursiveStepProvider() throws Exception {
		return (Table iTable) -> {
			return (Table t) -> {
				try {
					return LazyProjection.factory(
							LazyJoin.factory(
									new LazyFacade(t), 
									new LazyFacade(iTable),  
									new OnEquals(Electricity.customer, Electricity.customer),
									new OnSimilar(
											new PlusDateTime(Electricity.toTime, TIME_STEP), 
											Electricity.time, 
											LinearSimilarity.dateTimeSimilarityUntil(SIMILARITY_SCALE.toSeconds()))), 
							new Projection.To(Join.left(Electricity.customer), Electricity.customer),
							new Projection.To(Electricity.fromTime, Electricity.fromTime),
							new Projection.To(Electricity.time, Electricity.toTime),
							new Projection.To(new PlusInteger(Electricity.peaks, new Constant<Integer>(1)), Electricity.peaks));
				} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException
						| OnOperatornNotApplicableToSchemaException e) {
					throw new RuntimeException(e);
				}
			};
		};
	}

	@Override
	protected Function<Table, LazyExpression> postprocessProvider() throws Exception {
		return (Table iTable) -> {
			return LazyRestriction.factory(
					new LazyFacade(iTable),
					(rq.common.table.Record r) -> {
						int numOfPeaks = (Integer)r.getNoThrow(Electricity.peaks);
						if(numOfPeaks >= SEARCHED_NUMBER_OF_PEAKS) {
							return r.rank;
						}
						if(numOfPeaks == 1) {
							return 0.0d;
						}
						return Goguen.PRODUCT.apply(r.rank, (double)numOfPeaks / (double)SEARCHED_NUMBER_OF_PEAKS);
					});
		};
	}

	@Override
	public LazyExpression preprocess(LazyExpression iTable) {
		LazyExpression le = 
				LazyRestriction.factory(
						iTable, 
						peakRanker);

		return le;
	}
	
	private final BiFunction<Object, Object, Double> peakSimilarity = 
			LinearSimilarity.doubleSimilarityUntil(PEAK_SIMILARITY);
	private final Function<rq.common.table.Record, Double> peakRanker =
			(rq.common.table.Record r) -> {
				double value = (double)r.getNoThrow(Electricity.value);
				double moving_avg = (double)r.getNoThrow(Electricity.movingAvg);
				double div = value/moving_avg;
				if(div >= PEAK_MULTIPLIER) {
					return r.rank;
				}
				return Goguen.PRODUCT.apply(r.rank, peakSimilarity.apply(PEAK_SIMILARITY, div));
			};

}
