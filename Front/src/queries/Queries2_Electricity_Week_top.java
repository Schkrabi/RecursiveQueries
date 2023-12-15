package queries;

import java.time.Duration;
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
import rq.common.tools.AlgorithmMonitor;

@CallingArg("electricityWeekTop")
public class Queries2_Electricity_Week_top extends Queries2 {
	private Duration timeStep = Duration.ofDays(60);
	@QueryParameter("timeStep")
	public void setTimeStep(String timeStep) {
		this.timeStep = Duration.ofDays(Integer.parseInt(timeStep));
	}
	@QueryParameterGetter("timeStep")
	public String getTimeStep() {
		return this.timeStep.toString();
	}
	
	private Duration similarityScale = Duration.ofDays(20);
	@QueryParameter("similarityScale")
	public void setSimilarityScale(String scale) {
		this.similarityScale = Duration.ofDays(Integer.parseInt(scale));
	}
	@QueryParameterGetter("similarityScale")
	public String getSimilarityScale() {
		return this.similarityScale.toString();
	}
	
	private Double peakMultiplier =  1.2d;
	@QueryParameter("peakMultiplier")
	public void setPeakMultiplier(String peakMultiplier) {
		this.peakMultiplier = Double.parseDouble(peakMultiplier);
	}
	@QueryParameterGetter("peakMultiplier")
	public String getPeakMultiplier() {
		return this.peakMultiplier.toString();
	}
	
	private Double peakMultiplierAfterFirst = 1.1d;
	@QueryParameter("peakMultiplierAfterFirst")
	public void setPeakMultiplierAfterFirst(String peakMultiplier) {
		this.peakMultiplierAfterFirst = Double.parseDouble(peakMultiplier);
	}	
	@QueryParameterGetter("peakMultiplierAfterFirst")
	public String getPeakMultiplierAfterFirst() {
		return this.peakMultiplierAfterFirst.toString();
	}

	public Queries2_Electricity_Week_top(Class<? extends LazyRecursive> algorithm, AlgorithmMonitor monitor) {
		super(algorithm, monitor);
	}

	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("Electricity")
				.append("Week")
				.append("Top")
				.toString();
	}

	@Override
	protected Function<Table, LazyExpression> initialProvider() throws Exception {
		return (Table iTable) -> {
			try {
				return LazyProjection.factory(
						LazyRestriction.factory(
								new LazyFacade(iTable), 
								r -> (Double)r.getNoThrow(Electricity.value) > peakMultiplier * (Double)r.getNoThrow(Electricity.movingAvg) ? r.rank : 0.0d), 
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
			return (Table t) -> 
			{
				try {
					return LazyProjection.factory(
							LazyJoin.factory(
									new LazyFacade(t), 
									LazyRestriction.factory(
											new LazyFacade(iTable), 
											r -> (Double)r.getNoThrow(Electricity.value) > peakMultiplierAfterFirst * (Double)r.getNoThrow(Electricity.movingAvg) ? r.rank : 0.0d), 
									new OnEquals(Electricity.customer, Electricity.customer),
									new OnLesserThan(Electricity.toTime, Electricity.time),
									new OnSimilar(
											new PlusDateTime(Electricity.toTime, timeStep), 
											Electricity.time, 
											LinearSimilarity.dateTimeSimilarityUntil(similarityScale.toSeconds()))), 
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
		return (Table iTable) -> new LazyFacade(iTable);
	}

	@Override
	public LazyExpression preprocess(LazyExpression iTable) {
		LazyExpression le = 
				LazyRestriction.factory(
						iTable, 
						r -> (Double)r.getNoThrow(Electricity.value) > (Double)r.getNoThrow(Electricity.movingAvg) ? r.rank : 0.0d);

		return le;
	}

}
