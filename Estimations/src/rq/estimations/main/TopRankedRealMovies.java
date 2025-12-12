package rq.estimations.main;

import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.onOperators.Constant;
import rq.common.operators.Selection;
import rq.common.restrictions.GreaterThanOrEquals;
import rq.common.restrictions.InfimumAnd;
import rq.common.restrictions.LesserThanOrEquals;
import rq.common.restrictions.ProductAnd;
import rq.common.restrictions.Similar;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.Attribute;

public class TopRankedRealMovies extends Experiment {

	private Attribute Id = new Attribute("Id", java.lang.Integer.class);	
	private Attribute Movie_Name = new Attribute("Movie Name", rq.common.types.Str50.class);
	private Attribute Year_of_Release = new Attribute("Year of Release", java.lang.Double.class);
	private Attribute Watch_Time = new Attribute("Watch Time", java.lang.Double.class);
	private Attribute Movie_Rating = new Attribute("Movie Rating", java.lang.Double.class);	
	private Attribute Meatscore_of_movie = new Attribute("Meatscore of movie", java.lang.Double.class);
	private Attribute Votes = new Attribute("Votes", java.lang.Double.class);
	private Attribute Gross = new Attribute("Gross", java.lang.Double.class);
//	private Attribute Description = new Attribute("Description", rq.common.types.Str50.class);

	private BiFunction<Object, Object, Double> yearOfReleaseSimilarity_5 = LinearSimilarity.doubleSimilarityUntil(5d);
	private BiFunction<Object, Object, Double> watchTimeSimilarity_300 = LinearSimilarity.doubleSimilarityUntil(300d);
	private BiFunction<Object, Object, Double> watchTimeSimilarity_30 = LinearSimilarity.doubleSimilarityUntil(30d);
	private BiFunction<Object, Object, Double> ratingSimilarity_10 = LinearSimilarity.doubleSimilarityUntil(10d);
	private BiFunction<Object, Object, Double> ratingSimilarity_2 = LinearSimilarity.doubleSimilarityUntil(2d);
	private BiFunction<Object, Object, Double> metaScoreSimilarity_20 = LinearSimilarity.doubleSimilarityUntil(20d);
	private BiFunction<Object, Object, Double> votesSimilarity_5000 = LinearSimilarity.doubleSimilarityUntil(5000d);
	private BiFunction<Object, Object, Double> grossSimilarity_10 = LinearSimilarity.doubleSimilarityUntil(10d);
	
	private List<Integer> slices = List.of(5/*, 3, 8*/);
	private List<Integer> probes = List.of(5/*, 2, 0*/);
	
	private List<Attribute> numericAttributes = 
			List.of(Year_of_Release, 
					Watch_Time, 
					Movie_Rating, 
					Meatscore_of_movie, 
					Votes, 
					Gross);
	
	private List<Attribute> nominalAttributes =	List.of();
	
	Map<Attribute, Double> histSampleSize =
			Map.of( Year_of_Release, 5d,
					Watch_Time, 10d,
					Movie_Rating, 1d,
					Meatscore_of_movie, 10d,
					Votes, 100_000d,
					Gross, 50d);
	
	Map<Attribute, Collection<Integer>> nConsVls =
			Map.of(Year_of_Release, List.of(/*3, 4,*/ 5),
					Watch_Time, List.of(/*3, 4,*/ 5),
					Movie_Rating, List.of(/*1,*/ 2),
					Meatscore_of_movie, List.of(/*2, 3,*/ 4),
					Votes, List.of(/*1, 2,*/ 3),
					Gross, List.of(/*1, 2,*/ 3));
	
	Map<Attribute, Collection<Double>> _prtVls =
			Map.of(Year_of_Release, List.of(0.8d, 0.6d, 0.5d),
					Watch_Time, List.of(0.8d, 0.6d, 0.5d),
					Movie_Rating, List.of(0.8d, 0.6d, 0.5d),
					Meatscore_of_movie, List.of(0.8d, 0.6d, 0.5d),
					Votes, List.of(0.8d, 0.6d, 0.5d),
					Gross, List.of(0.8d, 0.6d, 0.5d));
	
	Map<Attribute, List<Integer>> intervals =
			Map.of( Year_of_Release, List.of(/*20, 40,*/ 60),
					Watch_Time, List.of(/*30, 60,*/ 120),
					Movie_Rating, List.of(/*3, 5,*/ 10),
					Meatscore_of_movie, List.of(/*3, 5,*/ 10),
					Votes, List.of(/*25, 50,*/ 75),
					Gross, List.of(/*20, 40,*/ 60));
	
	Map<Attribute, Double> similarities = 
//			Map.of( Year_of_Release, 5.0d,
//					Watch_Time, 30.0d,
//					Movie_Rating, 2.0d,
//					Meatscore_of_movie, 20.0d,
//					Votes, 5000.0d,
//					Gross, 10.0d);
//			Map.of( Year_of_Release, 2.5d,
//					Watch_Time, 5.0d,
//					Movie_Rating, 0.5d,
//					Meatscore_of_movie, 5.0d,
//					Votes, 5000.0d,
//					Gross, 10.0d);
			Map.of( Year_of_Release, 10d,
					Watch_Time, 20d,
					Movie_Rating, 0.2d,
					Meatscore_of_movie, 20d,
					Votes, 200_000d,
					Gross, 100.0d);
	
	Map<Attribute, List<Integer>> estSamples =
			Map.of( Year_of_Release, List.of(/*3, 5,*/ 8),
					Watch_Time, List.of(/*3, 5,*/ 8),
					Movie_Rating, List.of(/*3, 5,*/ 8),
					Meatscore_of_movie, List.of(/*3, 5,*/ 8),
					Votes, List.of(/*3, 5,*/ 8),
					Gross, List.of(/*3, 5,*/ 8));
	
	private TopRankedRealMovies() {}
	private static TopRankedRealMovies singleton = new TopRankedRealMovies();
	public static TopRankedRealMovies instance() {
		return singleton;
	}

	@Override
	protected Path folder() {
		return Path.of("C:\\Users\\r.skrabal\\Documents\\private-r.skrabal\\Java\\RecursiveQueries\\estimation_experiments\\TopRankedRealMoviesDataset");
		//return Path.of("./estimation_experiments/TopRankedRealMoviesDataset");
	}

	@Override
	protected String primaryDataFileName() {
		return "Top Ranked Real Movies Dataset.csv";
	}

	@Override
	protected String preparedDataName() {
		return "prepared";
	}

	@Override
	protected TabularExpression prepareDataQuery(Table primaryData) {
		return new Selection(primaryData,
				new ProductAnd(
						new Similar(this.Movie_Rating, new Constant<Double>(5d), this.ratingSimilarity_10),
						new Similar(this.Watch_Time, new Constant<Double>(150d), this.watchTimeSimilarity_300)));
	}

	@Override
	protected List<Integer> slices() {
		return this.slices;
	}

	@Override
	protected List<Attribute> numericAttributes() {
		return this.numericAttributes;
	}

	@Override
	protected List<Attribute> nominalAttributes() {
		return this.nominalAttributes;
	}

	@Override
	protected double histSampleSize(Attribute a) {
		return this.histSampleSize.get(a);
	}

	@Override
	protected List<Integer> intervals(Attribute a) {
		return this.intervals.get(a);
	}

	@Override
	protected double similarUntil(Attribute a) {
		return this.similarities.get(a);
	}

	@Override
	protected List<Integer> probes() {
		return this.probes;
	}

	@Override
	protected List<Integer> estSamples(Attribute a) {
		return this.estSamples.get(a);
	}

	@Override
	protected Map<String, TabularExpression> prepareSubDataQueries(Table preparedData) {
		return Map.of(
				"s1", new Selection(preparedData,
						new InfimumAnd(
								new GreaterThanOrEquals(Gross, new Constant<Double>(1d)),
								new LesserThanOrEquals(Gross, new Constant<Double>(10d)))),
				"s2", new Selection(preparedData,
						new InfimumAnd(
								new LesserThanOrEquals(Year_of_Release, new Constant<Double>(2010d)),
								new GreaterThanOrEquals(Year_of_Release, new Constant<Double>(1995d)))),
				"s3", new Selection(preparedData,
						new InfimumAnd(
								new LesserThanOrEquals(Watch_Time, new Constant<Double>(240d)),
								new GreaterThanOrEquals(Watch_Time, new Constant<Double>(180d)))),
				"s4", new Selection(preparedData,
						new InfimumAnd(
								new LesserThanOrEquals(Movie_Rating, new Constant<Double>(8d)),
								new GreaterThanOrEquals(Movie_Rating, new Constant<Double>(6d)))),
				"s5", new Selection(preparedData,
						new InfimumAnd(
								new LesserThanOrEquals(Meatscore_of_movie, new Constant<Double>(40d)),
								new GreaterThanOrEquals(Meatscore_of_movie, new Constant<Double>(20d))))
				);
	}

	@Override
	protected Map<String, List<Attribute>> smallData() {
		var m = new LinkedHashMap<String, List<Attribute>>();
		Stream.of("s1", "s2", "s3", "s4", "s5").forEach(s -> m.put(this.subdataName(s), List.of()));
		return m;
	}

	@Override
	protected List<Attribute> projectionAttributes() {
		return List.of(this.Id, this.Movie_Name);
	}

	@Override
	protected long seed() {
		return 547341576;
	}

	@Override
	protected Map<Attribute, Collection<Integer>> nConsideredValues() {
		return nConsVls;
	}

	@Override
	protected Map<Attribute, Collection<Double>> paretRatios() {
		return _prtVls;
	}

}
