package queries;

import java.util.function.BiFunction;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.operators.AbstractJoin;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRecursiveUnrestricted;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.Attribute;
import rq.common.table.LazyFacade;
import rq.common.tools.Counter;
import rq.common.types.Str10;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnSimilar;

@CallingArg("toloker")
public class Queries_Toloker extends Queries{
	
	public Queries_Toloker(Algorithm algorithm, Counter counter) {
		super(algorithm, counter);
	}
	
	public static Queries factory(Algorithm algorithm, Counter counter) {
		return new Queries_Toloker(algorithm, counter);
	}

	private final double RATE_SIMILARITY = 0.15d;
	private final int INIT_IS_BANNED = 0;
	private final double INIT_APPROVED_RATE_BELOW = 0.0d;
	private final int STARTING_NODE =
			1;
//			2;
//			324;
//			2013;
//			2118;
//			3889;
//			4173;
//			5175;
//			5406;
//			5737;
//			6300;
//			6919;
//			8707;
//			9203;
//			9372;
//			10316;
//			11027;

	
	//Specific similarities
	private final String EDU_NONE = "none";
	private final String EDU_BASIC = "basic";
	private final String EDU_MIDDLE = "middle";
	private final String EDU_HIGH = "high";
	
	private BiFunction<Object, Object, Double> educationSimilarity = 
			(Object o1, Object o2) -> {
				Str10 s1 = (Str10)o1;
				Str10 s2 = (Str10)o2;
				switch(s1.getInner()) {
				case EDU_NONE:
					switch(s2.getInner()) {
						case EDU_NONE: return 1.0d;
						case EDU_BASIC: return 0.66d;
						case EDU_MIDDLE: return 0.33d;
						case EDU_HIGH: return 0.0d;
						default:throw new RuntimeException(s2.getInner() + " is not a valid education value");
					}
				case EDU_BASIC:
					switch(s2.getInner()){
						case EDU_NONE: return 0.66d;
						case EDU_BASIC: return 1.0d;
						case EDU_MIDDLE: return 0.66d;
						case EDU_HIGH: return 0.33d;
						default:throw new RuntimeException(s2.getInner() + " is not a valid education value");
					}	
				case EDU_MIDDLE:
					switch(s2.getInner()) {
						case EDU_NONE: return 0.33d;
						case EDU_BASIC: return 0.66d;
						case EDU_MIDDLE: return 1.0d;
						case EDU_HIGH: return 0.66d;
						default:throw new RuntimeException(s2.getInner() + " is not a valid education value");
					}
				case EDU_HIGH:
					switch(s2.getInner()) {
						case EDU_NONE: return 0.0d;
						case EDU_BASIC: return 0.33d;
						case EDU_MIDDLE: return 0.66d;
						case EDU_HIGH: return 1.0d;
						default:throw new RuntimeException(s2.getInner() + " is not a valid education value");
					}
				default: throw new RuntimeException(s1.getInner() + " is not a valid education value");
				}
			};
	
	//File coluns 
	private final Attribute source = new Attribute("source", Integer.class);
	private final Attribute target = new Attribute("target", Integer.class);
	private final Attribute sourceApprovedRate = new Attribute("source_approved_rate", Double.class);
	private final Attribute sourceSkippedRate = new Attribute("source_skipped_rate", Double.class);
	private final Attribute sourceExpiredRate = new Attribute("source_expired_rate", Double.class);
	private final Attribute sourceRejectedRate = new Attribute("source_rejected_rate", Double.class);
	private final Attribute sourceEducation = new Attribute("source_education", Str10.class);
	private final Attribute sourceEnglishProfile = new Attribute("source_english_profile", Integer.class);
	private final Attribute sourceEnglishTested = new Attribute("source_english_tested", Integer.class);
	private final Attribute sourceBanned = new Attribute("source_banned", Integer.class);
	private final Attribute targetApprovedRate = new Attribute("target_approved_rate", Double.class);
	private final Attribute targetSkippedRate = new Attribute("target_skipped_rate", Double.class);
	private final Attribute targetExpiredRate = new Attribute("target_expired_rate", Double.class);
	private final Attribute targetRejectedRate = new Attribute("target_rejected_rate", Double.class);
	private final Attribute targetEductation = new Attribute("target_education", Str10.class);
	private final Attribute targetEnglishProfile = new Attribute("target_english_profile", Integer.class);
	private final Attribute targetEnglishTested = new Attribute("target_english_tested", Integer.class);
	private final Attribute targetBanned = new Attribute("target_banned", Integer.class);

	@Override
	protected LazyExpression prepareUnrestricted(LazyExpression iTable) {
		return iTable;
	}

	@Override
	protected LazyExpression prepareTopK(LazyExpression iTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected LazyExpression prepareTransformed(LazyExpression iTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected TabularExpression queryUnterstricted(Table iTable) {
		TabularExpression exp = 
				LazyRecursiveUnrestricted.factory(
						LazyRestriction.factory(
								new LazyFacade(iTable), 
								r -> //((Integer)r.getNoThrow(source)) == STARTING_NODE ? r.rank : 0.0d),
										(Integer)r.getNoThrow(sourceBanned) == INIT_IS_BANNED
									&&	(Double)r.getNoThrow(sourceApprovedRate) <= INIT_APPROVED_RATE_BELOW ? r.rank : 0.0d),
						(Table t) ->
							{
								try {
									return LazyProjection.factory(
											LazyJoin.factory(
												new LazyFacade(t), 
												new LazyFacade(iTable), 
												new OnEquals(target, source),
												new OnSimilar(sourceApprovedRate, sourceApprovedRate, LinearSimilarity.doubleSimilarityUntil(RATE_SIMILARITY)),
												new OnSimilar(sourceRejectedRate, sourceRejectedRate, LinearSimilarity.doubleSimilarityUntil(RATE_SIMILARITY)),
												new OnSimilar(sourceSkippedRate, sourceSkippedRate, LinearSimilarity.doubleSimilarityUntil(RATE_SIMILARITY)),
												new OnSimilar(sourceExpiredRate, sourceExpiredRate, LinearSimilarity.doubleSimilarityUntil(RATE_SIMILARITY))/*,
												new OnSimilar(sourceEducation, sourceEducation, educationSimilarity)*/),
											new Projection.To(AbstractJoin.right(source), source),
											new Projection.To(AbstractJoin.right(target), target),
											new Projection.To(AbstractJoin.right(sourceApprovedRate), sourceApprovedRate),
											new Projection.To(AbstractJoin.right(sourceRejectedRate), sourceRejectedRate),
											new Projection.To(AbstractJoin.right(sourceSkippedRate), sourceSkippedRate),
											new Projection.To(AbstractJoin.right(sourceExpiredRate), sourceExpiredRate),
											new Projection.To(AbstractJoin.right(sourceEducation), sourceEducation),
											new Projection.To(AbstractJoin.right(sourceEnglishProfile), sourceEnglishProfile),
											new Projection.To(AbstractJoin.right(sourceEnglishTested), sourceEnglishTested),
											new Projection.To(AbstractJoin.right(sourceBanned), sourceBanned),
											new Projection.To(AbstractJoin.right(targetApprovedRate), targetApprovedRate),
											new Projection.To(AbstractJoin.right(targetSkippedRate), targetSkippedRate),
											new Projection.To(AbstractJoin.right(targetExpiredRate), targetExpiredRate),
											new Projection.To(AbstractJoin.right(targetRejectedRate), targetRejectedRate),
											new Projection.To(AbstractJoin.right(targetEductation), targetEductation),
											new Projection.To(AbstractJoin.right(targetEnglishProfile), targetEnglishProfile),
											new Projection.To(AbstractJoin.right(targetEnglishTested), targetEnglishTested),
											new Projection.To(AbstractJoin.right(targetBanned), targetBanned));
								} catch (DuplicateAttributeNameException | OnOperatornNotApplicableToSchemaException | RecordValueNotApplicableOnSchemaException e) {
									throw new RuntimeException(e);
								}
							},
							this.recordCounter);
		
		return exp;
	}

	@Override
	protected TabularExpression queryTopK(Table iTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected TabularExpression queryTransformed(Table iTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected TabularExpression postprocessUnrestricted(Table iTable) {
		return iTable;
	}

	@Override
	protected TabularExpression postprocessTopK(Table iTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected TabularExpression postprocessTransformed(Table iTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String algIdentificator() {
		// TODO Auto-generated method stub
		return null;
	}
}
