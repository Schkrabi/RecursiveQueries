package rq.estimations.main;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.Attribute;
import rq.common.types.Str10;
import rq.common.types.Str50;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.exceptions.DuplicateHeaderWriteException;
import rq.files.io.TableWriter;
import rq.common.onOperators.Constant;
import rq.common.operators.Restriction;
import rq.common.operators.Selection;
import rq.common.restrictions.Similar;
import rq.common.restrictions.Equals;
import rq.common.restrictions.GreaterThanOrEquals;
import rq.common.restrictions.InfimumAnd;
import rq.common.restrictions.LesserThanOrEquals;
import rq.common.restrictions.ProductAnd;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;

public class VideoGameSales extends Experiment {	
	List<Integer> slices = List.of(3, 8);
	List<Integer> probes = List.of(5, 2, 0);
	
	Attribute name = new Attribute("Name", Str50.class);
	Attribute platform = new Attribute("Platform", Str10.class);
	Attribute year_of_release = new Attribute("Year_of_Release", Double.class);
	Attribute genre = new Attribute("Genre", Str10.class);
	Attribute publisher = new Attribute("Publisher", Str50.class);
	Attribute na_sales = new Attribute("NA_Sales", Double.class);
	Attribute eu_sales = new Attribute("EU_Sales", Double.class);
	Attribute jp_sales = new Attribute("JP_Sales", Double.class);
	Attribute other_sales = new Attribute("Other_Sales", Double.class);
	Attribute global_sales = new Attribute("Global_Sales", Double.class);
	Attribute critic_score = new Attribute("Critic_Score", Double.class);
	Attribute critic_count = new Attribute("Critic_Count", Double.class);
	Attribute user_score = new Attribute("User_Score", Double.class);
	Attribute user_count = new Attribute("User_Count", Double.class);
	Attribute developer = new Attribute("Developer", Str50.class);
	Attribute rating = new Attribute("Rating", Str10.class);
	
	BiFunction<Object, Object, Double> yearSimilarity_20 = LinearSimilarity.doubleSimilarityUntil(20d);
	BiFunction<Object, Object, Double> yearSimilarity_3 = LinearSimilarity.doubleSimilarityUntil(3d);
	BiFunction<Object, Object, Double> scoreSimilarity_100 = LinearSimilarity.doubleSimilarityUntil(101d);
	BiFunction<Object, Object, Double> salesSimilarity = LinearSimilarity.doubleSimilarityUntil(2d);
	BiFunction<Object, Object, Double> scoreSimilarity_30 = LinearSimilarity.doubleSimilarityUntil(30d);
	BiFunction<Object, Object, Double> countSimilarity = LinearSimilarity.doubleSimilarityUntil(50d);
	
	List<Attribute> numericAttributes = 
			List.of(year_of_release,
					na_sales,
					eu_sales,
					jp_sales,
					other_sales,
					global_sales,
					critic_score,
					critic_count,
					user_score,
					user_count);
	
	List<Attribute> nominalAttributes = 
			List.of(platform,
					genre,
					publisher,
					developer,
					rating);
	
	Map<Attribute, Double> sampleSizes = 
			Map.of( year_of_release, 1d,
					na_sales, 1d,
					eu_sales, 1d,
					jp_sales, 1d,
					other_sales, 1d,
					global_sales, 2d,
					critic_score, 5d,
					critic_count, 10d,
					user_score, 5d,
					user_count, 10d);
	
	Map<Attribute, List<Integer>> intervals = 
			Map.of( year_of_release, List.of(7, 14),
					na_sales, List.of(20, 40),
					eu_sales, List.of(20, 40),
					jp_sales, List.of(20, 40),
					other_sales, List.of(20, 40),
					global_sales, List.of(20, 40),
					critic_score, List.of(20, 40),
					critic_count, List.of(20, 40),
					user_score, List.of(20, 40),
					user_count, List.of(40, 80));
	
	Map<Attribute, List<Integer>> samples = 
			Map.of( year_of_release, List.of(17, 8),
					na_sales, List.of(40, 20),
					eu_sales, List.of(40, 20),
					jp_sales, List.of(40, 20),
					other_sales, List.of(40, 20),
					global_sales, List.of(40, 20),
					critic_score, List.of(20, 4),
					critic_count, List.of(40, 10),
					user_score, List.of(20, 4),
					user_count, List.of(40, 10));
			
	Map<Attribute, BiFunction<Object, Object, Double>> similarities = 
			Map.of(	year_of_release, yearSimilarity_3,
					na_sales, salesSimilarity,
					eu_sales, salesSimilarity,
					jp_sales, salesSimilarity,
					other_sales, salesSimilarity,
					global_sales, salesSimilarity,
					critic_score, scoreSimilarity_30,
					critic_count, countSimilarity,
					user_score, scoreSimilarity_30,
					user_count, countSimilarity);
	
	Map<Attribute, List<Object>> numericalSelValues = 
			Map.of( year_of_release, List.of(1980d, 1985d, 1990d, 1995d, 2000d, 2005d, 2010d, 2015d, 2020d),
					na_sales, List.of(0.5d, 1d, 3d, 6d, 9d, 12d, 15d, 18d, 20d),
					eu_sales, List.of(0.5d, 1d, 3d, 6d, 9d, 12d, 15d, 18d, 20d),
					jp_sales, List.of(0.5d, 1d, 3d, 6d, 9d, 12d, 15d, 18d, 20d),
					other_sales, List.of(0.5d, 1d, 3d, 6d, 9d, 12d, 15d, 18d, 20d),
					global_sales, List.of(0.5d, 2d, 6d, 12d, 18d, 24d, 30d, 36d, 40d),
					critic_score, List.of(5d, 15d, 50d, 70d, 75d, 80d, 85d, 90d, 95d),
					critic_count, List.of(10d, 30d, 50d, 70d, 100d, 150d, 200d),
					user_score, List.of(5d, 15d, 50d, 70d, 75d, 80d, 85d, 90d, 95d),
					user_count, List.of(10d, 30d, 50d, 70d, 100d, 150d, 200d));

	private VideoGameSales() {};
	private static VideoGameSales singleton = new VideoGameSales();
	public static VideoGameSales instance() {
		return singleton;
	}
	
	@Override
	protected Path folder() {
//		return Path.of("C:\\Users\\r.skrabal\\Documents\\Mine\\Java\\RecursiveQueries\\estimation_experiments\\VideoGameSales");
		return Path.of("./VideoGameSales");
	}
	@Override
	protected String primaryDataFileName() {
		return "Video_Games.csv";
	}
	@Override
	protected String preparedDataName() {
		return "prepared";
	}

	@Override
	protected TabularExpression prepareDataQuery(Table primaryData) {
		return new Selection(
				primaryData,
				new ProductAnd(
						new Similar(year_of_release, new Constant<Double>(2000d), yearSimilarity_20),
						new Similar(critic_score, new Constant<Double>(100d), scoreSimilarity_100)));
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
		return this.sampleSizes.get(a);
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
		return this.samples.get(a);
	}

	@Override
	protected List<Object> queryValues(Attribute a) {
		return this.numericalSelValues.get(a);
	}

	@Override
	protected Map<String, TabularExpression> prepareSubDataQueries(Table preparedData) {
		return Map.of(
				"s1", new Selection(preparedData,
								new InfimumAnd(
										new GreaterThanOrEquals(year_of_release, new Constant<Double>(2000d)),
										new LesserThanOrEquals(year_of_release, new Constant<Double>(2005d)))),
				"s2", new Selection(preparedData,
						new InfimumAnd(
								new GreaterThanOrEquals(global_sales, new Constant<Double>(1d)),
								new LesserThanOrEquals(global_sales, new Constant<Double>(20d)))),
				"s3", new Selection(preparedData,
						new InfimumAnd(
								new GreaterThanOrEquals(na_sales, new Constant<Double>(10d)),
								new LesserThanOrEquals(na_sales, new Constant<Double>(30d)))),
				"s4", new Selection(preparedData,
						new InfimumAnd(
								new GreaterThanOrEquals(eu_sales, new Constant<Double>(4d)),
								new LesserThanOrEquals(eu_sales, new Constant<Double>(6d)))),
				"s5", new Selection(preparedData,
						new InfimumAnd(
								new GreaterThanOrEquals(jp_sales, new Constant<Double>(5d)),
								new LesserThanOrEquals(jp_sales, new Constant<Double>(7d))))
				);
	}
	
	private final String pokemonGames = "pokemon.csv";
	
	public void pokemonGames() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Restriction(this.getPreparedData(),
				 r -> ((Str50)r.getNoThrow(this.name)).getInner().toLowerCase().contains("pokemon") ? r.rank : 0d);
		
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(pokemonGames));
	}
	
	private String blizzard = "blizzard.csv";
	
	private void blizzard() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Selection(this.getPreparedData(),
					new Equals(this.developer, new Constant<Str50>(Str50.factory("Blizzard Entertainment"))));
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(blizzard));
	}
	
	private String _3do = "3do.csv";
	
	private void _3do() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Selection(this.getPreparedData(),
					new Equals(this.publisher, new Constant<Str50>(Str50.factory("3DO"))));
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(_3do));
	}

	@Override
	protected Map<String, List<Attribute>> smallData() {
		try {
			this.pokemonGames();
			this.blizzard();
			this._3do();
		} catch (IOException | ClassNotInContextException | DuplicateHeaderWriteException e) {
			throw new RuntimeException(e);
		}
		return Map.of(pokemonGames, List.of(this.platform, this.publisher),
						blizzard, List.of(this.platform, this.publisher),
						_3do, List.of(this.platform, this.publisher));
	}

	@Override
	protected List<Attribute> projectionAttributes() {
		return List.of(this.name, this.developer, this.genre, this.platform, this.publisher, this.developer, this.rating);
	}
}
