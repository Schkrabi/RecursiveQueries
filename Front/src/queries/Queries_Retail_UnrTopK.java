package queries;

import java.time.Duration;
import java.util.function.BiFunction;

import data.Retail;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.onOperators.Constant;
import rq.common.operators.Join;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRecursiveTopK;
import rq.common.operators.LazyRecursiveUnrestricted;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.operators.Restriction;
import rq.common.similarities.LinearSimilarity;
import rq.common.tools.Counter;
import rq.common.table.LazyFacade;
import rq.common.table.Record;
import rq.common.table.TopKTable;
import rq.common.onOperators.*;

@CallingArg("retail_unrtop")
public class Queries_Retail_UnrTopK extends Queries {
	
	//I would like to find out what items do have periodic peaks
	
	private final Double MAX_QTY_MULT = 2.5d;
	private final Double MIN_QTY_MULT = 1.5d;
	private final Duration STEP = Duration.ofDays(365);
	private final Duration SIMILARITY_SCALE = Duration.ofDays(14);
	
	private final int K = 50_000;

	public Queries_Retail_UnrTopK(Algorithm algorithm, Counter counter) {
		super(algorithm, counter);
	}
	
	public static Queries factory(Algorithm algorithm, Counter counter) {
		return new Queries_Retail_UnrTopK(algorithm, counter);
	}

	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("retail")
				.append("_unrXtop")
				.append("_maxQtyMukt").append(MAX_QTY_MULT)
				.append("_minQtyMult").append(MIN_QTY_MULT)
				.append("_step=").append(STEP.toString())
				.append("_simScale=").append(SIMILARITY_SCALE.toString())
				.append("_K=").append(K)
				.toString();
	}
	
	private LazyExpression prepareData(LazyExpression iTable) {
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

	@Override
	protected LazyExpression prepareUnrestricted(LazyExpression iTable) {
		return this.prepareData(iTable);
	}

	@Override
	protected LazyExpression prepareTopK(LazyExpression iTable) {
		return this.prepareData(iTable);
	}

	@Override
	protected LazyExpression prepareTransformed(LazyExpression iTable) {
		throw this.algorithmNotSupportedException();
	}

	@Override
	protected TabularExpression queryUnterstricted(Table iTable) {
		TabularExpression exp = null;
		
		try {
			exp = LazyRecursiveUnrestricted.factory(
					LazyProjection.factory(
						new LazyFacade(iTable), 
						new Projection.To(Retail.invoiceDate, Retail.fromTime),
						new Projection.To(Retail.invoiceDate, Retail.toTime),
						new Projection.To(new Constant<Integer>(1), Retail.peaks),
						new Projection.To(Retail.stockCode, Retail.stockCode)), 
					(Table t) ->{
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
					},
					this.recordCounter);
		} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException e) {
			throw new RuntimeException(e);
		}
		
		return exp;
	}

	@Override
	protected TabularExpression queryTopK(Table iTable) {
TabularExpression exp = null;
		
		try {
			exp = LazyRecursiveTopK.factory(
					LazyProjection.factory(
						new LazyFacade(iTable),
						new Projection.To(Retail.invoiceDate, Retail.fromTime),
						new Projection.To(Retail.invoiceDate, Retail.toTime),
						new Projection.To(new Constant<Integer>(1), Retail.peaks),
						new Projection.To(Retail.stockCode, Retail.stockCode)), 
					(Table t) ->{
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
					},
					K,
					this.recordCounter);
		} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException e) {
			throw new RuntimeException(e);
		}
		
		return exp;
	}

	@Override
	protected TabularExpression queryTransformed(Table iTable) {
		throw this.algorithmNotSupportedException();
	}

	@Override
	protected TabularExpression postprocessUnrestricted(Table iTable) {
		TabularExpression exp = null;
		
		exp = new Restriction(
				iTable,
				r -> r.rank,
				(s, k) -> TopKTable.factory(s, K));
		
		return exp;
	}

	@Override
	protected TabularExpression postprocessTopK(Table iTable) {
		return iTable;
	}

	@Override
	protected TabularExpression postprocessTransformed(Table iTable) {
		throw this.algorithmNotSupportedException();
	}

}
