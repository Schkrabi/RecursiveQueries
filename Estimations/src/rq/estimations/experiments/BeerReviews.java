package rq.estimations.experiments;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.io.contexts.ClassNotInContextException;
import rq.common.onOperators.Constant;
import rq.common.operators.Selection;
import rq.common.restrictions.Equals;
import rq.common.restrictions.GreaterThanOrEquals;
import rq.common.restrictions.InfimumAnd;
import rq.common.restrictions.LesserThanOrEquals;
import rq.common.restrictions.ProductAnd;
import rq.common.restrictions.Similar;
import rq.common.similarities.ISimilarity;
import rq.common.similarities.LinearSimilarities;
import rq.common.table.Attribute;
import rq.common.types.Str50;
import rq.estimations.framework.Experiment;
import rq.estimations.main.Workbench;
import rq.files.exceptions.DuplicateHeaderWriteException;
import rq.files.io.TableWriter;

public class BeerReviews extends Experiment {
	
	Path folder = Path.of("C:\\Users\\r.skrabal\\Documents\\private-r.skrabal\\Java\\RecursiveQueries\\estimation_experiments\\Beer Reviews");
	//Path folder = Path.of("./estimation_experiments/Beer_Reviews");
	String beer_reviews_csv = "beer_reviews.nona.csv";
	Path beer_reviews_path = folder.resolve(beer_reviews_csv);
	String query1 = "q1";
	Path query1Folder = folder.resolve(query1);
	String query1FileName = Workbench.dataFileName(beer_reviews_csv, query1);
	Path query1File = query1Folder.resolve(query1FileName);
	Path query1HistFolder = query1Folder.resolve("hist");
	Path query1HistFile = query1HistFolder.resolve("beer_reviews.csv.q1.csv.3.hist");
	Path query1EstFolder = query1Folder.resolve("est");
	String query1StatFileName = query1FileName + ".stat";
	
	//Attributes
	Attribute<Str50> brewery_name = new Attribute<>("brewery_name", rq.common.types.Str50.class);
	Attribute<Str50> beer_style = new Attribute<>("beer_style", rq.common.types.Str50.class);
	Attribute<Str50> review_profilename = new Attribute<>("review_profilename", rq.common.types.Str50.class);
	Attribute<Integer> review_time = new Attribute<>("review_time",java.lang.Integer.class);
	Attribute<Double> review_taste = new Attribute<>("review_taste",java.lang.Double.class);
	Attribute<Str50> beer_name = new Attribute<>("beer_name", rq.common.types.Str50.class);
	Attribute<Integer> brewery_id = new Attribute<>("brewery_id", java.lang.Integer.class);
	Attribute<Double> review_aroma = new Attribute<>("review_aroma", java.lang.Double.class);
	Attribute<Double> beer_abv = new Attribute<>("beer_abv", java.lang.Double.class);
	Attribute<Double> review_overall = new Attribute<>("review_overall", java.lang.Double.class);
	Attribute<Double> review_appearance = new Attribute<>("review_appearance", java.lang.Double.class);
	Attribute<Integer> beer_beerid = new Attribute<>("beer_beerid", java.lang.Integer.class);
	Attribute<Double> review_palate = new Attribute<>("review_palate", java.lang.Double.class);
	
	List<Attribute<Double>> numericAttributes = 
			List.of(review_taste, review_aroma, review_overall, review_appearance, review_palate);
	List<Attribute<?>> nominalAttributes = 
			List.of(brewery_name, beer_style, review_profilename);
	
	//Estimation setup
	List<Integer> slices = List.of(5/*3, 8*/);
	List<Integer> probes = List.of(/*0,*/ 2);
	Map<Attribute<?>, List<Integer>> intervals =
			Map.of( review_taste, List.of(3),
					review_aroma, List.of(3),
					review_overall, List.of(3),
					review_appearance, List.of(3),
					review_palate, List.of(3));
	Map<Attribute<Double>, List<Integer>> samples = 
			Map.of( review_taste, List.of(3),
					review_aroma, List.of(3),
					review_overall, List.of(3),
					review_appearance, List.of(3),
					review_palate, List.of(3));
	Map<Attribute<Double>, Double> histSampleSizes = 
			Map.of( review_taste, 1d,
					review_aroma, 1d,
					review_overall, 1d,
					review_appearance, 1d,
					review_palate, 1d);

	//Sample query setup
	ISimilarity<Double> reviewSimilarity_2 = LinearSimilarities.doubleSimilarityUntil(2d);
	ISimilarity<Double> reviewSimilarity_5 = LinearSimilarities.doubleSimilarityUntil(5d);
	
	Map<Attribute<Double>, ISimilarity<Double>> similarities = 
			Map.of( review_taste, LinearSimilarities.doubleSimilarityUntil(2.0d),
					review_aroma, LinearSimilarities.doubleSimilarityUntil(2.0d),
					review_overall, LinearSimilarities.doubleSimilarityUntil(2.0d),
					review_appearance, LinearSimilarities.doubleSimilarityUntil(2.0d),
					review_palate, LinearSimilarities.doubleSimilarityUntil(2.0d));
	Map<Attribute<Double>, Collection<Integer>> _nOfCnsVls =
			Map.of( review_taste, List.of(20),
					review_aroma, List.of(20),
					review_overall, List.of(20),
					review_appearance, List.of(20),
					review_palate, List.of(20));
	
	Map<Attribute<Double>, Collection<Double>> _parRts = 
			Map.of( review_taste, List.of(0.8d/*, 0.6d, 0.5d*/),
					review_aroma, List.of(0.8d/*, 0.6d, 0.5d*/),
					review_overall, List.of(0.8d/*, 0.6d, 0.5d*/),
					review_appearance, List.of(0.8d/*, 0.6d, 0.5d*/),
					review_palate, List.of(0.8d/*, 0.6d, 0.5d*/));
	
	//Singleton
	private BeerReviews() {}
	private static BeerReviews singleton = new BeerReviews();
	public static BeerReviews instance() {
		return singleton;
	}

	@Override
	protected Path folder() {
		return folder;
	}

	@Override
	protected String primaryDataFileName() {
		return beer_reviews_csv;
	}

	@Override
	protected String preparedDataName() {
		return "prepared";
	}

	@Override
	protected TabularExpression prepareDataQuery(Table primaryData) {
		return new Selection(primaryData, 
				new ProductAnd(
						new Similar<>(review_overall, new Constant<Double>(1.d), reviewSimilarity_5),
						new Similar<>(review_appearance, new Constant<Double>(4.d), reviewSimilarity_5)));
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
		return this.histSampleSizes.get(a);
	}

	@Override
	protected List<Integer> intervals(Attribute<?> a) {
		return this.intervals.get(a);
	}

//	@Override
//	protected double similarUntil(Attribute<Double> a) {
//		return this.similarities.get(a);
//	}

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
							new LesserThanOrEquals<>(this.review_palate, new Constant<Double>(2.0d)),
							new GreaterThanOrEquals<>(this.review_palate, new Constant<Double>(1.0d)))),
				"s2", new Selection(preparedData,
						new InfimumAnd(
								new LesserThanOrEquals<>(this.review_taste,  new Constant<Double>(2d)),
								new GreaterThanOrEquals<>(this.review_taste, new Constant<Double>(1.5d)))),
				"s3", new Selection(preparedData,
						new InfimumAnd(
								new LesserThanOrEquals<>(this.review_aroma,  new Constant<Double>(2.5d)),
								new GreaterThanOrEquals<>(this.review_aroma, new Constant<Double>(2d)))),
				"s4", new Selection(preparedData,
						new InfimumAnd(
								new LesserThanOrEquals<>(this.review_appearance,  new Constant<Double>(2.5d)),
								new GreaterThanOrEquals<>(this.review_appearance, new Constant<Double>(2d)))),
				"s5", new Selection(preparedData,
						new InfimumAnd(
								new LesserThanOrEquals<>(this.review_overall,  new Constant<Double>(1.5d)),
								new GreaterThanOrEquals<>(this.review_overall, new Constant<Double>(1d))))
				);
	}

	private final String plzenskyPrazdroj = "plzen";
	
	public void plzenskyPrazdroj() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Selection(this.getPreparedData(),
				new Equals<>(this.brewery_name, new Constant<Str50>(Str50.factory("Plzensky Prazdroj, a. s."))));
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(plzenskyPrazdroj));
	}
	
	public final String leenanau = "leenanau";
	
	public void leenanau() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Selection(this.getPreparedData(),
				new Equals<>(this.brewery_name, new Constant<Str50>(Str50.factory("Leelanau Brewing Company"))));
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(leenanau));
	}
	
	public final String brasserie = "brasserie";
	
	public void brasserie() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		var q = new Selection(this.getPreparedData(),
				new Equals<>(this.brewery_name, new Constant<Str50>(Str50.factory("Brasserie Dunham"))));
		var data = q.eval();
		TableWriter.spit(data, this.preparedDataFolder().resolve(brasserie));
	}
	
	@Override
	protected Map<String, List<Attribute<?>>> smallData() {
		try {
			this.plzenskyPrazdroj();
			this.leenanau();
			this.brasserie();
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
		return Map.of(
				this.plzenskyPrazdroj, List.of(this.beer_style, this.review_profilename),
				this.leenanau, List.of(this.beer_style, this.review_profilename),
				this.brasserie, List.of(this.beer_style, this.review_profilename));
	}

	@Override
	protected List<Attribute<?>> projectionAttributes() {
		return List.of(this.brewery_id, this.beer_beerid);
	}

	@Override
	protected long seed() {
		return 331226761;
	}

	@Override
	protected Map<Attribute<Double>, Collection<Integer>> nConsideredValues() {
		return this._nOfCnsVls;
	}

	@Override
	protected Map<Attribute<Double>, Collection<Double>> paretRatios() {
		return this._parRts;
	}

	@Override
	protected ISimilarity<Double> similarity(Attribute<Double> a) {
		return this.similarities.get(a);
	}

	@Override
	protected Map<Attribute<Double>, ISimilarity<Double>> numericalSimilarityMap() {
		return this.similarities;
	}
}
