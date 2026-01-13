package rq.estimations.experiments;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import rq.common.similarities.ISimilarity;
import rq.common.similarities.LinearSimilarities;
import rq.common.table.Attribute;
import rq.common.types.Str10;
import rq.common.types.Str50;
import rq.estimations.framework.Experiment;
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
import rq.common.io.contexts.ClassNotInContextException;

public class VideoGameSales extends Experiment {	
	List<Integer> slices = List.of(5/*, 3, 8*/);
	List<Integer> probes = List.of(5/*, 2, 0*/);
	
	Attribute<Str50> name = new Attribute<>("Name", Str50.class);
	Attribute<Str10> platform = new Attribute<>("Platform", Str10.class);
	Attribute<Double> year_of_release = new Attribute<>("Year_of_Release", Double.class);
	Attribute<Str10> genre = new Attribute<>("Genre", Str10.class);
	Attribute<Str50> publisher = new Attribute<>("Publisher", Str50.class);
	Attribute<Double> na_sales = new Attribute<>("NA_Sales", Double.class);
	Attribute<Double> eu_sales = new Attribute<>("EU_Sales", Double.class);
	Attribute<Double> jp_sales = new Attribute<>("JP_Sales", Double.class);
	Attribute<Double> other_sales = new Attribute<>("Other_Sales", Double.class);
	Attribute<Double> global_sales = new Attribute<>("Global_Sales", Double.class);
	Attribute<Double> critic_score = new Attribute<>("Critic_Score", Double.class);
	Attribute<Double> critic_count = new Attribute<>("Critic_Count", Double.class);
	Attribute<Double> user_score = new Attribute<>("User_Score", Double.class);
	Attribute<Double> user_count = new Attribute<>("User_Count", Double.class);
	Attribute<Str50> developer = new Attribute<>("Developer", Str50.class);
	Attribute<Str10> rating = new Attribute<>("Rating", Str10.class);
	
	ISimilarity<Double> yearSimilarity_20 = LinearSimilarities.doubleSimilarityUntil(20d);
	ISimilarity<Double> yearSimilarity_3 = LinearSimilarities.doubleSimilarityUntil(3d);
	ISimilarity<Double> scoreSimilarity_100 = LinearSimilarities.doubleSimilarityUntil(101d);
	ISimilarity<Double> salesSimilarity = LinearSimilarities.doubleSimilarityUntil(2d);
	ISimilarity<Double> scoreSimilarity_30 = LinearSimilarities.doubleSimilarityUntil(30d);
	ISimilarity<Double> countSimilarity = LinearSimilarities.doubleSimilarityUntil(50d);
	
	List<Attribute<Double>> numericAttributes = 
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
	
	List<Attribute<?>> nominalAttributes = 
			List.of(platform,
					genre,
					publisher,
					developer,
					rating);
	
	Map<Attribute<Double>, Double> sampleSizes = 
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
	
	Map<Attribute<Double>, Collection<Integer>> _nOfCnsVls =
			Map.of( year_of_release, List.of(20),
					na_sales, List.of(20),
					eu_sales, List.of(20),
					jp_sales, List.of(20),
					other_sales, List.of(20),
					global_sales, List.of(20),
					critic_score, List.of(20),
					critic_count, List.of(20),
					user_score, List.of(20),
					user_count, List.of(20));
	
	Map<Attribute<Double>, Collection<Double>> _prtRts =
			Map.of( year_of_release, List.of(0.8d/*, 0.6d, 0.5d*/),
					na_sales, List.of(0.8d/*, 0.6d, 0.5d*/),
					eu_sales, List.of(0.8d/*, 0.6d, 0.5d*/),
					jp_sales, List.of(0.8d/*, 0.6d, 0.5d*/),
					other_sales, List.of(0.8d/*, 0.6d, 0.5d*/),
					global_sales, List.of(0.8d/*, 0.6d, 0.5d*/),
					critic_score, List.of(0.8d/*, 0.6d, 0.5d*/),
					critic_count, List.of(0.8d/*, 0.6d, 0.5d*/),
					user_score, List.of(0.8d/*, 0.6d, 0.5d*/),
					user_count, List.of(0.8d/*, 0.6d, 0.5d*/));
	
	Map<Attribute<?>, List<Integer>> intervals = 
			Map.of( year_of_release, List.of(/*7,*/ 14),
					na_sales, List.of(/*20,*/ 40),
					eu_sales, List.of(/*20,*/ 40),
					jp_sales, List.of(/*20,*/ 40),
					other_sales, List.of(/*20,*/ 40),
					global_sales, List.of(/*20,*/ 40),
					critic_score, List.of(/*20,*/ 40),
					critic_count, List.of(/*20,*/ 40),
					user_score, List.of(/*20,*/ 40),
					user_count, List.of(/*40,*/ 80));
	
	Map<Attribute<Double>, List<Integer>> samples = 
			Map.of( year_of_release, List.of(17/*, 8*/),
					na_sales, List.of(40/*, 20*/),
					eu_sales, List.of(40/*, 20*/),
					jp_sales, List.of(40/*, 20*/),
					other_sales, List.of(40/*, 20*/),
					global_sales, List.of(40/*, 20*/),
					critic_score, List.of(40/*, 20*/),
					critic_count, List.of(40/*, 20*/),
					user_score, List.of(40/*, 20*/),
					user_count, List.of(40/*, 20*/));
			
	Map<Attribute<Double>, Double> similarities = 
			Map.of(	year_of_release, 3.0d,
					na_sales, 2.0d,
					eu_sales, 2.0d,
					jp_sales, 2.0d,
					other_sales, 2.0d,
					global_sales, 2.0d,
					critic_score, 30.0d,
					critic_count, 50.0d,
					user_score, 30.0d,
					user_count, 50.0d);

	private VideoGameSales() {};
	private static VideoGameSales singleton = new VideoGameSales();
	public static VideoGameSales instance() {
		return singleton;
	}
	
	@Override
	protected Path folder() {
		return Path.of("C:\\Users\\r.skrabal\\Documents\\private-r.skrabal\\Java\\RecursiveQueries\\estimation_experiments\\VideoGameSales\\");
		//return Path.of("./estimation_experiments/VideoGameSales");
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
						new Similar<>(year_of_release, new Constant<Double>(2000d), yearSimilarity_20),
						new Similar<>(critic_score, new Constant<Double>(100d), scoreSimilarity_100)));
	}

	@Override
	protected List<Integer> slices() {
		return this.slices;
	}

	@Override
	protected List<Attribute<Double>> numericAttributes() {
		return this.numericAttributes;
	}

	@Override
	protected List<Attribute<?>> nominalAttributes() {
		return this.nominalAttributes;
	}

	@Override
	protected double histSampleSize(Attribute<Double> a) {
		return this.sampleSizes.get(a);
	}

	@Override
	protected List<Integer> intervals(Attribute<?> a) {
		return this.intervals.get(a);
	}

	@Override
	protected double similarUntil(Attribute<Double> a) {
		return this.similarities.get(a);
	}

	@Override
	protected List<Integer> probes() {
		return this.probes;
	}

	@Override
	protected List<Integer> estSamples(Attribute<?> a) {
		return this.samples.get(a);
	}

	@Override
	protected Map<String, TabularExpression> prepareSubDataQueries(Table preparedData) {
		return Map.of(
				"s1", new Selection(preparedData,
								new InfimumAnd(
										new GreaterThanOrEquals<>(year_of_release, new Constant<Double>(2000d)),
										new LesserThanOrEquals<>(year_of_release, new Constant<Double>(2005d)))),
				"s2", new Selection(preparedData,
						new InfimumAnd(
								new GreaterThanOrEquals<>(global_sales, new Constant<Double>(1d)),
								new LesserThanOrEquals<>(global_sales, new Constant<Double>(20d)))),
				"s3", new Selection(preparedData,
						new InfimumAnd(
								new GreaterThanOrEquals<>(na_sales, new Constant<Double>(10d)),
								new LesserThanOrEquals<>(na_sales, new Constant<Double>(30d)))),
				"s4", new Selection(preparedData,
						new InfimumAnd(
								new GreaterThanOrEquals<>(eu_sales, new Constant<Double>(4d)),
								new LesserThanOrEquals<>(eu_sales, new Constant<Double>(6d)))),
				"s5", new Selection(preparedData,
						new InfimumAnd(
								new GreaterThanOrEquals<>(jp_sales, new Constant<Double>(5d)),
								new LesserThanOrEquals<>(jp_sales, new Constant<Double>(7d))))
				);
	}
	
	private final String pokemonGames = "pokemon.csv";
	
	public void pokemonGames() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Restriction(this.getPreparedData(),
				 r -> r.getNoThrow(this.name).getInner().toLowerCase().contains("pokemon") ? r.rank : 0d);
		
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(pokemonGames));
	}
	
	private String blizzard = "blizzard.csv";
	
	private void blizzard() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Selection(this.getPreparedData(),
					new Equals<>(this.developer, new Constant<Str50>(Str50.factory("Blizzard Entertainment"))));
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(blizzard));
	}
	
	private String _3do = "3do.csv";
	
	private void _3do() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Selection(this.getPreparedData(),
					new Equals<>(this.publisher, new Constant<Str50>(Str50.factory("3DO"))));
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(_3do));
	}

	@Override
	protected Map<String, List<Attribute<?>>> smallData() {
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
	protected List<Attribute<?>> projectionAttributes() {
		return List.of(this.name, this.developer, this.genre, this.platform, this.publisher, this.developer, this.rating);
	}

	@Override
	protected long seed() {
		return 113266111;
	}

	@Override
	protected Map<Attribute<Double>, Collection<Integer>> nConsideredValues() {
		return this._nOfCnsVls;
	}

	@Override
	protected Map<Attribute<Double>, Collection<Double>> paretRatios() {
		return this._prtRts;
	}
}
