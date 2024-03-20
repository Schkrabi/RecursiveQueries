package rq.estimations.main;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.onOperators.Constant;
import rq.common.operators.Selection;
import rq.common.restrictions.Equals;
import rq.common.restrictions.GreaterThanOrEquals;
import rq.common.restrictions.InfimumAnd;
import rq.common.restrictions.LesserThanOrEquals;
import rq.common.restrictions.Or;
import rq.common.restrictions.ProductAnd;
import rq.common.restrictions.Similar;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.Attribute;
import rq.common.types.Str50;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.exceptions.DuplicateHeaderWriteException;
import rq.files.io.TableWriter;

public class AmazonBookScrappings extends Experiment {
	
	private Attribute Id = new Attribute("Id", java.lang.Integer.class);
//	private Attribute Title = new Attribute("Title", rq.common.types.Str50.class);
	private Attribute Author = new Attribute("Author", rq.common.types.Str50.class);
	private Attribute Main_Genre = new Attribute("Main Genre", rq.common.types.Str50.class);
	private Attribute Sub_Genre = new Attribute("Sub Genre", rq.common.types.Str50.class);
	private Attribute Type = new Attribute("Type", rq.common.types.Str50.class);
	private Attribute Price = new Attribute("Price", java.lang.Double.class);
	private Attribute Rating = new Attribute("Rating", java.lang.Double.class);
	private Attribute No_of_People_rated = new Attribute("No. of People rated", java.lang.Double.class);
//	private Attribute URLs = new Attribute("URLs", rq.common.types.Str50.class);
	
	private BiFunction<Object, Object, Double> priceSimilarity_20_000 = LinearSimilarity.doubleSimilarityUntil(20_000d);
	private BiFunction<Object, Object, Double> priceSimilarity_50 = LinearSimilarity.doubleSimilarityUntil(100d);
	private BiFunction<Object, Object, Double> ratingSimilarity_5 = LinearSimilarity.doubleSimilarityUntil(5d);
	private BiFunction<Object, Object, Double> ratingSimilarity_2 = LinearSimilarity.doubleSimilarityUntil(2d);
	private BiFunction<Object, Object, Double> NoOfPeopleRatedSimilarity_1000 = LinearSimilarity.doubleSimilarityUntil(1_000d);
	
	private List<Integer> slices = List.of(8, 3);
	private List<Integer> probes = List.of(0, 3, 5);
	
	private List<Attribute> numericalAttributes = 
			List.of(Price, Rating, No_of_People_rated);
	private List<Attribute> nominalAttributes = 
			List.of(Main_Genre, Sub_Genre, Type);
	
	private List<Attribute> projectionAttributes = 
			List.of(Id, Author, Main_Genre, Sub_Genre, Type);
	
	private Map<Attribute, Double> histSampleSizes = 
			Map.of( Price, 500d,
					Rating, 1d,
					No_of_People_rated, 10_000d);
	
	private Map<Attribute, List<Integer>> intervals = 
			Map.of( Price, List.of(35, 70, 140),
					Rating, List.of(3, 5, 10),
					No_of_People_rated, List.of(5, 20, 80));
	
	private Map<Attribute, BiFunction<Object, Object, Double>> similarities =
			Map.of( Price, this.priceSimilarity_50,
					Rating, this.ratingSimilarity_2,
					No_of_People_rated, this.NoOfPeopleRatedSimilarity_1000);
	
	private Map<Attribute, List<Integer>> estSamples = 
			Map.of( Price, List.of(50, 100, 300),
					Rating, List.of(2, 3, 5),
					No_of_People_rated, List.of(100, 500, 1000));
	
	private Map<Attribute, List<Object>> queryValues = 
			Map.of( Price, Stream.concat(Stream.of(50d, 100d, 500d), Stream.iterate(1000d, x -> x + 2000d).limit(17)).collect(Collectors.toList()),
					Rating, List.of(1d, 2d, 3d, 4d, 5d),
					No_of_People_rated, Stream.iterate(0d, x -> x + 10_000d).limit(50).collect(Collectors.toList()));

	private AmazonBookScrappings() {}
	private static AmazonBookScrappings singleton = new AmazonBookScrappings();
	public static AmazonBookScrappings instance() {
		return singleton;
	}

	@Override
	protected Path folder() {
		//return Path.of("C:\\Users\\r.skrabal\\Documents\\Mine\\Java\\RecursiveQueries\\estimation_experiments\\Amazon_Books_Scraping");
		return Path.of("./Amazon_Books_Scraping");
	}

	@Override
	protected String primaryDataFileName() {
		return "Books_df.csv";
	}

	@Override
	protected String preparedDataName() {
		return "prepared";
	}

	@Override
	protected TabularExpression prepareDataQuery(Table primaryData) {
		return new Selection(primaryData,
				new ProductAnd(
						new Similar(this.Price, new Constant<Double>(15_000d), this.priceSimilarity_20_000),
						new Similar(this.Rating, new Constant<Double>(5d), this.ratingSimilarity_5)));
	}

	@Override
	protected List<Integer> slices() {
		return this.slices;
	}

	@Override
	protected List<Attribute> numericAttributes() {
		return this.numericalAttributes;
	}

	@Override
	protected List<Attribute> nominalAttributes() {
		return this.nominalAttributes;
	}

	@Override
	protected double histSampleSize(Attribute a) {
		return this.histSampleSizes.get(a);
	}

	@Override
	protected List<Integer> intervals(Attribute a) {
		return this.intervals.get(a);
	}

	@Override
	protected BiFunction<Object, Object, Double> similarity(Attribute a) {
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
	protected List<Object> queryValues(Attribute a) {
		return this.queryValues.get(a);
	}

	@Override
	protected Map<String, TabularExpression> prepareSubDataQueries(Table preparedData) {
		return Map.of(
				"s1", new Selection(preparedData,
						new InfimumAnd(
								new GreaterThanOrEquals(Price, new Constant<Double>(750d)),
								new LesserThanOrEquals(Price, new Constant<Double>(1000d)))),
				"s2", new Selection(preparedData,
						new InfimumAnd(
								new GreaterThanOrEquals(No_of_People_rated, new Constant<Double>(500d)),
								new LesserThanOrEquals(No_of_People_rated, new Constant<Double>(1000d)))),
				"s3", new Selection(preparedData,
						new InfimumAnd(
								new GreaterThanOrEquals(Rating, new Constant<Double>(1d)),
								new LesserThanOrEquals(Rating, new Constant<Double>(2d)))),
				"s4", new Selection(preparedData,
						new Or(
								new Equals(Main_Genre, new Constant<Str50>(Str50.factory("Engineering"))),
								new Equals(Main_Genre, new Constant<Str50>(Str50.factory("History"))))),
				"s5", new Selection(preparedData,
						new Or(
								new Equals(Type, new Constant<Str50>(Str50.factory("Paperback"))),
								new Equals(Type, new Constant<Str50>(Str50.factory("Hardcover")))))
				);
	}
	
	private final String computerNtechnology = "computerNtechnology";
	
	private void computerNtechnology() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Selection(this.getPreparedData(),
				new Equals(this.Sub_Genre, new Constant<Str50>(Str50.factory("Computers & Technology"))));
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(computerNtechnology));
	}
	
	private final String romanticSuspense = "romanticSuspense";
	
	private void romanticSuspense() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Selection(this.getPreparedData(),
				new Equals(this.Sub_Genre, new Constant<Str50>(Str50.factory("Romantic Suspense"))));
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(romanticSuspense));
	}
	
	private final String hobbiesNgames = "hobbiesNgames";
	
	private void hobbiesNgames() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Selection(this.getPreparedData(),
				new Equals(this.Sub_Genre, new Constant<Str50>(Str50.factory("Hobbies & Games"))));
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(hobbiesNgames));
	}

	@Override
	protected Map<String, List<Attribute>> smallData() {
		try {
			this.computerNtechnology();
			this.romanticSuspense();
			this.hobbiesNgames();
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
		return Map.of(
				this.computerNtechnology, List.of(this.Type, this.Main_Genre),
				this.romanticSuspense, List.of(this.Type, this.Main_Genre),
				this.hobbiesNgames, List.of(this.Type, this.Main_Genre)
				);
	}

	@Override
	protected List<Attribute> projectionAttributes() {
		return this.projectionAttributes;
	}

	@Override
	protected long seed() {
		return 863522926;
	}
}
