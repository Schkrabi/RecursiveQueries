package rq.estimations.main;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.io.contexts.ClassNotInContextException;
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
import rq.files.exceptions.DuplicateHeaderWriteException;
import rq.files.io.TableWriter;

public class AnimeDataset2023 extends Experiment {

	private Attribute anime_id = new Attribute("anime_id", java.lang.Integer.class);
//	private Attribute Name = new Attribute("Name", rq.common.types.Str50.class);	
//	private Attribute English_name = new Attribute("English name", rq.common.types.Str50.class);	
//	private Attribute Other_name = new Attribute("Other name", rq.common.types.Str50.class);	
	private Attribute Score = new Attribute("Score", java.lang.Double.class);	
//	private Attribute Synopsis = new Attribute("Synopsis", rq.common.types.Str50.class);	
	private Attribute Type = new Attribute("Type", rq.common.types.Str10.class);	
	private Attribute Episodes = new Attribute("Episodes", java.lang.Double.class);	
//	private Attribute Aired = new Attribute("Aired", rq.common.types.Str10.class);	
//	private Attribute Premiered = new Attribute("Premiered", rq.common.types.Str10.class);	
	private Attribute Status = new Attribute("Status", rq.common.types.Str50.class);	
//	private Attribute Producers = new Attribute("Producers", rq.common.types.Str50.class);	
//	private Attribute Licensors = new Attribute("Licensors", rq.common.types.Str50.class);	
//	private Attribute Studios = new Attribute("Studios", rq.common.types.Str50.class);	
	private Attribute Source = new Attribute("Source", rq.common.types.Str50.class);	
//	private Attribute Duration = new Attribute("Duration", rq.common.types.Str50.class);	
	private Attribute Rating = new Attribute("Rating", rq.common.types.Str50.class);	
	private Attribute Rank = new Attribute("Rank", java.lang.Double.class);	
	private Attribute Popularity = new Attribute("Popularity", java.lang.Double.class);	
	private Attribute Favorites = new Attribute("Favorites", java.lang.Double.class);	
	private Attribute Scored_By = new Attribute("Scored By", java.lang.Double.class);	
	private Attribute Members = new Attribute("Members", java.lang.Double.class);	
//	private Attribute Image_URL = new Attribute("Image URL", rq.common.types.Str50.class);	
	private Attribute Genre = new Attribute("Genre", rq.common.types.Str50.class);
	
	private BiFunction<Object, Object, Double> scoreSimilarity_10 = LinearSimilarity.doubleSimilarityUntil(10d);
	private BiFunction<Object, Object, Double> scoreSimilarity_2 = LinearSimilarity.doubleSimilarityUntil(2d);
	private BiFunction<Object, Object, Double> episodesSimilarity_100 = LinearSimilarity.doubleSimilarityUntil(200d);
	private BiFunction<Object, Object, Double> rankSimilarity_2000 = LinearSimilarity.doubleSimilarityUntil(2000d);
	private BiFunction<Object, Object, Double> popularitySimilarity_2000 = LinearSimilarity.doubleSimilarityUntil(2000d);
	private BiFunction<Object, Object, Double> favoritesSimilarity_200_000 = LinearSimilarity.doubleSimilarityUntil(200_000d);
	private BiFunction<Object, Object, Double> favoritesSimilarity_20_000 = LinearSimilarity.doubleSimilarityUntil(20_000d);
	private BiFunction<Object, Object, Double> scoredBySimilarity_200_000 = LinearSimilarity.doubleSimilarityUntil(200_000d);
	private BiFunction<Object, Object, Double> membersSimilarity_200_000 = LinearSimilarity.doubleSimilarityUntil(200_000d);
	
	private List<Integer> probes = List.of(/*0, 3,*/ 5);
	
	private List<Attribute> numericalAttributes = 
			List.of(Score,
					Episodes,
					Rank,
					Popularity,
					Favorites,
					Scored_By,
					Members);
	
	//Producers, Licensors and Studios are not considered there, since we do not have a resonable similarity for them.
	private List<Attribute> nominalAttributes =
			List.of(Type,
					Status,
					Source,
					Rating,
					Genre);
	
	private List<Attribute> projectionAttributes = 
			List.of(anime_id,
					Type,
					Status,
					Source,
					Rating,
					Genre);
	
	private Map<Attribute, Double> histSampleSizes =
			Map.of( Score, 1d,
					Episodes, 100d,
					Rank, 1000d,
					Popularity, 1000d,
					Favorites, 10_000d,
					Scored_By, 100_000d,
					Members, 100_000d);
	
	private Map<Attribute, Collection<Integer>> _nOfCnsVls =
			Map.of(Score, List.of(20),
					Episodes, List.of(20),
					Rank, List.of(20),
					Popularity, List.of(20),
					Favorites, List.of(20),
					Scored_By, List.of(20),
					Members, List.of(20));
	
	private Map<Attribute, Collection<Double>> _prtRts = 
			Map.of(Score, List.of(.8d/*, .6d, .5d*/),
					Episodes, List.of(.8d/*, .6d, .5d*/),
					Rank, List.of(.8d/*, .6d, .5d*/),
					Popularity, List.of(.8d/*, .6d, .5d*/),
					Favorites, List.of(.8d/*, .6d, .5d*/),
					Scored_By, List.of(.8d/*, .6d, .5d*/),
					Members, List.of(.8d/*, .6d, .5d*/));
	
	private Map<Attribute, List<Integer>> intervals =
			Map.of( Score, List.of(/*5, 10,*/ 20),
					Episodes, List.of(/*15, 30,*/ 60),
					Rank, List.of(/*20, 40,*/ 80),
					Popularity, List.of(/*10, 20,*/ 40),
					Favorites, List.of(/*10, 20,*/ 40),
					Scored_By, List.of(/*12, 25,*/ 50),
					Members, List.of(/*18, 35,*/ 70));
	
	private Map<Attribute, Double> similarities =
			Map.of( Score, 2.0d,
					Episodes, 150.0d,
					Rank, 2000.0d,
					Popularity, 2000.0d,
					Favorites, 20000.0d,
					Scored_By, 200000.0d,
					Members, 200000.0d);
	
	private Map<Attribute, List<Integer>> estSamples =
			Map.of( Score, List.of(/*3, 5,*/ 10),
					Episodes, List.of(/*7, 15,*/ 30),
					Rank, List.of(/*10, 20,*/ 40),
					Popularity, List.of(/*5, 10,*/ 20),
					Favorites, List.of(/*5, 10,*/ 20),
					Scored_By, List.of(/*6, 12,*/ 25),
					Members, List.of(/*9, 18,*/ 35));
	
	
	private AnimeDataset2023() {}
	private static AnimeDataset2023 singleton = new AnimeDataset2023();
	public static AnimeDataset2023 instance() {
		return singleton;
	}

	@Override
	protected Path folder() {
		return Path.of("C:\\Users\\r.skrabal\\Documents\\private-r.skrabal\\Java\\RecursiveQueries\\estimation_experiments\\Anime_Dataset_2023");
		//return Path.of("./estimation_experiments/Anime_Dataset_2023");
	}

	@Override
	protected String primaryDataFileName() {
		return "anime-dataset-2023.genres.csv";
	}

	@Override
	protected String preparedDataName() {
		return "prepared";
	}

	@Override
	protected TabularExpression prepareDataQuery(Table primaryData) {
		return new Selection(primaryData,
				new ProductAnd(new Similar(this.Score, new Constant<Double>(5d), this.scoreSimilarity_10),
						new Similar(this.Favorites, new Constant<Double>(100_000d), this.favoritesSimilarity_200_000)));
	}

	@Override
	protected List<Integer> slices() {
		return List.of(5/*, 3, 8*/);
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
										new GreaterThanOrEquals(Score, new Constant<Double>(7d)),
										new LesserThanOrEquals(Score, new Constant<Double>(8d)))),
				"s2", new Selection(preparedData,
						new InfimumAnd(
								new GreaterThanOrEquals(Episodes, new Constant<Double>(60d)),
								new LesserThanOrEquals(Episodes, new Constant<Double>(100d)))),
				"s3", new Selection(preparedData,
						new Or(
								new Equals(this.Rating, new Constant<Str50>(Str50.factory("Rx - Hentai"))), 
								new Equals(this.Rating, new Constant<Str50>(Str50.factory("R+ - Mild Nudity"))))),
				"s4", new Selection(preparedData,
						new Or(
								new Equals(this.Genre, new Constant<Str50>(Str50.factory("Sci-Fi"))), 
								new Equals(this.Genre, new Constant<Str50>(Str50.factory("Horror"))))),
				"s5", new Selection(preparedData,
						new Or(
								new Equals(this.Genre, new Constant<Str50>(Str50.factory("Action"))), 
								new Equals(this.Genre, new Constant<Str50>(Str50.factory("Fantasy")))))						
				);
	}
	
	private final String erotica = "erotica";
	
	private void erotica() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Selection(this.getPreparedData(),
				new Equals(this.Genre, new Constant<Str50>(Str50.factory("Erotica"))));
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(erotica));
	}
	
	private final String suspense = "suspense";
	
	private void suspense() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Selection(this.getPreparedData(),
				new Equals(this.Genre, new Constant<Str50>(Str50.factory("Suspense"))));
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(suspense));
	}
	
	private final String gourmet = "gourmet";
	
	private void gourmet() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Selection(this.getPreparedData(),
				new Equals(this.Genre, new Constant<Str50>(Str50.factory("Gourmet"))));
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(gourmet));
	}

	@Override
	protected Map<String, List<Attribute>> smallData() {
		try {
			this.erotica();
			this.suspense();
			this.gourmet();
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
		return Map.of(
				this.erotica, List.of(this.Type, this.Status, this.Source, this.Rating),
				this.suspense, List.of(this.Type, this.Status, this.Source, this.Rating),
				this.gourmet, List.of(this.Type, this.Status, this.Source, this.Rating));
	}

	@Override
	protected List<Attribute> projectionAttributes() {
		return this.projectionAttributes;
	}

	@Override
	protected long seed() {
		return 114115395;
	}

	@Override
	protected Map<Attribute, Collection<Integer>> nConsideredValues() {
		return this._nOfCnsVls;
	}

	@Override
	protected Map<Attribute, Collection<Double>> paretRatios() {
		return this._prtRts;
	}
}
