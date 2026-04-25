package rq.estimations.framework;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.opencsv.exceptions.CsvValidationException;

import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.NotSubschemaException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.SchemaNotEqualException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.io.contexts.ClassNotInContextException;
import rq.common.similarities.ISimilarity;
import rq.common.statistic.AttributeHistogram;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.MostCommonValues;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SampledHistogram;
import rq.common.table.Attribute;
import rq.estimations.contracts.RestrictionExperimentContract;
import rq.estimations.main.Workbench;
import rq.files.contracts.EstimationExperimentContract;
import rq.files.contracts.QueryGenerationStrategy;
import rq.files.exceptions.ColumnOrderingNotInitializedException;
import rq.files.exceptions.DuplicateHeaderWriteException;
import rq.files.helpers.JsonSerializer;
import rq.files.io.TableReader;
import rq.files.io.TableWriter;

public abstract class Experiment {

	protected Experiment() {}
	
	protected final boolean USE_RANKED_TABLE_AS_PRIMARY_DATA = false;
	
	/** Base folder for the experiments. Must contain primary data file */
	protected abstract Path folder();
	/** File containing the primary data. Primary data is unranked */
	protected abstract String primaryDataFileName();
	/** Name of the prepared data. Used to build folder structure */
	protected abstract String preparedDataName();
	
	protected Map<Attribute<Double>, Collection<Double>> getQueryValues(){
		return Map.of();
	}
	protected QueryGenerationStrategy getQueryGenerationStrategy() {
		return QueryGenerationStrategy.IntervalBasedParet;
	}
	
	private RestrictionExperiment _restrictionExperiment = null;
	protected RestrictionExperiment restrictionExperiment() {
		if(this._restrictionExperiment == null) {
			var cnt = new RestrictionExperimentContract();
			cnt.setDataPath(this.preparedDataPath());
			cnt.setQueryCount((int)SAMPLE_QUERY_COUNT);
			cnt.setAttributes(this.numericAttributes());
			cnt.setSlices(this.slices());
			cnt.setConsideredValues(this.nConsideredValues());
			cnt.setParetRatios(this.paretRatios());
			cnt.setIntervals(this.IntervalsMany(this.numericAttributes()));
//			cnt.setSimilarUntil(this.similarsUntil(this.numericAttributes()));
			cnt.setRandom(this.getRand());
			cnt.setUseRankedDataAsPrimary(this.USE_RANKED_TABLE_AS_PRIMARY_DATA);
			cnt.setQueryGenerationStrategy(this.getQueryGenerationStrategy());
			cnt.setQueryValues(this.getQueryValues());
			cnt.setEstFolder(Workbench.estFolder(this.folder()));
			cnt.setHistFolder(Workbench.histFolder(this.folder()));
			cnt.setSimilarity(this.numericalSimilarityMap());
			cnt.setKnownConstantSignatures(this.restrictionEstimationKnownConstantSignatures());
			cnt.setUnknownConstantSignatures(this.restrictionEstimationUnknownConstantSignatures());
			
			this._restrictionExperiment = 
					new RestrictionExperiment(cnt);
		}
		return this._restrictionExperiment;
	}
	
	protected abstract Map<Attribute<Double>, Collection<Integer>> nConsideredValues();
	protected abstract Map<Attribute<Double>, Collection<Double>> paretRatios();
	
	/** Prepared data with ranks for experiments */
	private Table preparedData = null;
	
	/** Path to primary data file */
	protected Path primaryDataPath() {
		return this.folder().resolve(this.primaryDataFileName());
	}
	
	/** Folder where prepared data is stored */
	protected Path preparedDataFolder() {
		return this.folder().resolve(this.preparedDataName());
	}
	
	/** File name of the prepared data */
	protected String preparedDataFileName() {
		return Workbench.dataFileName(this.primaryDataFileName(), this.preparedDataName());
	}
	
	/** File where prepared data is persisted */
	protected Path preparedDataPath() {
		return this.preparedDataFolder()
				.resolve(this.preparedDataFileName());
	}
	
	/** Query that process primary data and introduces ranks to data. Executing this query creates preparedData */
	protected abstract TabularExpression prepareDataQuery(Table primaryData);
	
	/** Prepares primary data and creates prepared data */
	public void prepareData() throws IOException, CsvValidationException, ClassNotFoundException,
			DuplicateAttributeNameException, ColumnOrderingNotInitializedException, ClassNotInContextException,
			TableRecordSchemaMismatch, DuplicateHeaderWriteException {
		var tr = TableReader.open(this.primaryDataPath());
		var primaryData = tr.read();
		tr.close();
		
		if(USE_RANKED_TABLE_AS_PRIMARY_DATA)
		{
			var query = this.prepareDataQuery(primaryData);
			this.preparedData = query.eval();
		}
		else
		{
			this.preparedData = primaryData;
		}
		
		if(!Files.exists(this.preparedDataFolder())) {
			Files.createDirectory(this.preparedDataFolder());
		}
		
		var tw = TableWriter.open(Files.newOutputStream(this.preparedDataPath()));
		tw.write(this.preparedData);
		tw.close();
		
		this.biPreparedData();
		this.prepareJoins();
	}
	
	/** List of slices for the experiments */
	protected abstract List<Integer> slices();
	
	/** Path for histogram folder */
	protected Path preparedDataHistFolder() {
		return Workbench.histFolder(this.preparedDataPath());
	}
	
	/** List of nominal attributes */
	protected abstract List<Attribute<Double>> numericAttributes();
	/** List of numerical attributes */
	protected abstract List<Attribute<?>> nominalAttributes();
	
	/** Returns histogram sample size for given attribute */
	protected abstract double histSampleSize(Attribute<Double> a);
	
	/** Returns list of intervals used for given attribute */
	protected abstract List<Integer> intervals(Attribute<?> a);
	
	protected Map<Attribute<Double>, Collection<Integer>> IntervalsMany(Collection<Attribute<Double>> as){
		var m = new HashMap<Attribute<Double>, Collection<Integer>>();
		for(var a : as) {
			m.put(a, this.intervals(a));
		}
		return m;
	}
	
	/** Extracts statistics from prepared data */
	public void preparedDataStatistics() throws IOException {
		if(!Files.exists(this.preparedDataHistFolder())) {
			Files.createDirectory(this.preparedDataHistFolder());
		}
		
		//Rank histograms
		for(var slice : this.slices()) {
			var hist = new RankHistogram(slice);
			hist.gather(this.preparedData);
			hist.writeFile(this.preparedDataHistFolder()
					.resolve(Workbench.rankHistFileName(this.preparedDataFileName(), slice)));
		}
		
		//Nominal attribute histograms
		for (var a : Stream.concat(this.nominalAttributes().stream(), this.projectionAttributes().stream())
				.collect(Collectors.toList())) {
			var hist = new AttributeHistogram<>(a);
			hist.gather(this.preparedData);
			var json = JsonSerializer.instance().serialize(hist);
			Files.writeString(this.preparedDataHistFolder()
					.resolve(Workbench.histName(this.preparedDataFileName(), a.name)), json);
		}
		
		//Numerical attribure histograms
		for(var a : this.numericAttributes()) {
			var sampleSize = this.histSampleSize(a);
			var hist = new SampledHistogram<>(a, sampleSize);
			hist.gather(this.preparedData);
			hist.writeFile(this.preparedDataHistFolder()
					.resolve(Workbench.sampledHistName(this.preparedDataFileName(), a.name)));
			ResourceLoader.instance().getOrLoadSampledHistogram(this.preparedDataPath(), a);
			
			var attHist = new AttributeHistogram<>(a);
			attHist.gather(this.preparedData);
			Files.writeString(this.preparedDataHistFolder()
					.resolve(Workbench.histName(this.preparedDataFileName(), a.name)), 
					JsonSerializer.instance().serialize(attHist));
			
			var mcv = new MostCommonValues<>(a);
			mcv.gather(this.preparedData);
			mcv.writeFile(Workbench.mcvFile(this.preparedDataPath(), a));
			ResourceLoader.instance().getOrLoadMCV(this.preparedDataPath(), a);
			
			for(var i : this.intervals(a)) {
				var eqn = new EquinominalHistogram<>(a, i);
				eqn.gather(this.preparedData);
				eqn.writeFile(this.preparedDataHistFolder()
						.resolve(Workbench.eqnHistName(this.preparedDataFileName(), a.name, i)));
				ResourceLoader.instance().getOrLoadEqnHistogram(this.preparedDataPath(), a, i);
				
				var eqd = new EquidistantHistogram<>(a, i);
				eqd.gather(this.preparedData);
				eqd.writeFile(this.preparedDataHistFolder()
						.resolve(Workbench.eqdHistName(this.preparedDataFileName(), a.name, i)));
				ResourceLoader.instance().getOrLoadEqdHistogram(this.preparedDataPath(), a, i);
			}
		}
		this.biDataStatistics();
		this.prepareJoinStatistics();
	}
	
	/** Folder where the estimates reside */
	protected Path preparedDataEstFolder() {
		return this.preparedDataFolder().resolve("est");
	}
	
	/** Returns similarity used for queries and estimates of given attribute */
	protected abstract rq.common.similarities.ISimilarity<Double> similarity(Attribute<Double> a);
	
//	protected abstract double similarUntil(Attribute<Double> a);
	
//	protected Map<Attribute<Double>, Double> similarsUntil(Collection<Attribute<Double>> as){
//		var m = new HashMap<Attribute<Double>, Double>();
//		for(var a : as) {
//			m.put(a, this.similarUntil(a));
//		}
//		return m;
//	}
	
	/** List of tested number of probes in the experiment */
	protected abstract List<Integer> probes();
	
	/** List of stochastic estimation samples for given attribute */
	protected abstract List<Integer> estSamples(Attribute<?> a);
	
	/** Computes the estimates */
	public void estimates() throws IOException, ClassNotFoundException {
		if(!Files.exists(this.preparedDataEstFolder())) {
			Files.createDirectory(this.preparedDataEstFolder());
		}
		
//		this.biEstimates();
//		this.projection.estimate();
	}
	
	/** Folder where query results are preserved */
	Path queryFolder() {
		return this.preparedDataFolder().resolve("queries");
	}
	
	private Random rand = null;
	
	protected Random getRand() {
		if(this.rand == null) {
			this.rand = new Random(this.seed());
		}
		return this.rand;
	}
	
	public final long SAMPLE_QUERY_COUNT = 200;
	
	private Path sampledHistFile(Attribute<?> a) {
		return this.preparedDataHistFolder().resolve(
				Workbench.sampledHistName(this.preparedDataFileName(), a.name));
	}
	
	public <T extends Number> SampledHistogram<T> sampledHist(Attribute<T> a) throws ClassNotFoundException, IOException {
		return SampledHistogram.readFile(this.sampledHistFile(a));
	}
	
	private Path attHistFile(Attribute<?> a) {
		return this.preparedDataHistFolder().resolve(
				Workbench.histName(this.preparedDataFileName(), a.name));		
	}
	
	@SuppressWarnings("unchecked")
	public <T> AttributeHistogram<T> attHist(Attribute<T> a) {
		try {
			var h = JsonSerializer.instance().deserialize(Files.readString(this.attHistFile(a)), AttributeHistogram.class);
			return h;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/** Computes the queries  
	 * @throws DuplicateAttributeNameException 
	 * @throws NotSubschemaException 
	 * @throws OnOperatornNotApplicableToSchemaException 
	 * @throws ClassNotFoundException */
	public void queries() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException, SchemaNotEqualException, NotSubschemaException, DuplicateAttributeNameException, OnOperatornNotApplicableToSchemaException, ClassNotFoundException {		
		//Restriction queries are computed in JIT fashion
		//this.biQueries();
		//this.projection.query();
	}
	
	/** Folder with results */
	Path resultFolder() {
		return Workbench.resultDir(this.preparedDataPath());
	}
	
	/** Reloads the prepared data from files */
	public void reloadPreparedData() throws IOException, CsvValidationException, ClassNotFoundException, DuplicateAttributeNameException, ColumnOrderingNotInitializedException, ClassNotInContextException, TableRecordSchemaMismatch {
		var tr = TableReader.open(Files.newInputStream(this.preparedDataPath()));
		this.preparedData = tr.read();
		tr.close();
		
		for(var id : this.prepareSubDataQueries(this.preparedData).keySet()) {
			var str = TableReader.open(Files.newInputStream(this.subdataPath(id)));
			var table = str.read();
			str.close();
			
			this.bidata.put(id, table);
		}
		
		this.prepareJoins();
	}
	
	/** Gathers experiment results 
	 * @throws ClassNotFoundException */
	public void gatherData() throws IOException, ClassNotFoundException {		
		this.restrictionExperiment().gatherData();
		
//		this.biGather();
//		this.projection.gather();
	}
	
	/** Prepare data for biexperiments */
	protected abstract Map<String, TabularExpression> prepareSubDataQueries(Table preparedData);
	
	/** sub data file name */
	protected String subdataName(String identifier) {
		return new StringBuilder(this.primaryDataFileName())
				.append(".")
				.append(identifier)
				.append(".csv")
				.toString();
	}
	
	/** subdata path*/
	Path subdataPath(String identifier) {
		return this.preparedDataFolder().resolve(this.subdataName(identifier));
	}
	
	protected Map<String, Table> bidata = new LinkedHashMap<String, Table>();
	
	/** Prepares sub data */
	private void biPreparedData() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException {
		bidata = new LinkedHashMap<String, Table>();
		for(var e : this.prepareSubDataQueries(this.preparedData).entrySet()) {
			var id = e.getKey();
			var q = e.getValue();
			var table = q.eval();
			
			var tw = TableWriter.open(Files.newOutputStream(this.subdataPath(id)));
			tw.write(table);
			tw.close();
			
			bidata.put(id, table);
		}
	}
	
	/** Computes statistics for subdata data */
	private void biDataStatistics() throws IOException {
		for(var slice : this.slices()) {
			for(var e : this.bidata.entrySet()) {
				var hist = new RankHistogram(slice);
				hist.gather(e.getValue());
				hist.writeFile(this.preparedDataHistFolder()
					.resolve(Workbench.rankHistFileName(this.subdataName(e.getKey()), slice)));
			}
		}
	}
	
	/** Computes binary operation estimations 
	 * @throws ClassNotFoundException */
	public void biEstimates() throws IOException, ClassNotFoundException {
		this.union.estimate();
		this.intersection.estimate();
		
		for(var join : this.joins) {
			join.estimate();
		}
	}
	
	/** Computes binary operation queries 
	 * @throws OnOperatornNotApplicableToSchemaException */
	public void biQueries() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException, SchemaNotEqualException, OnOperatornNotApplicableToSchemaException {
		this.union.query();
		this.intersection.query();
		
		for(var join : this.joins) {
			join.query();
		}
	}
	
	private String joinResultFileName() {
		return new StringBuilder()
				.append(this.preparedDataFileName())
				.append(".joins.stat.csv")
				.toString();
	}
	
	private String crossjoinResultFileName() {
		return new StringBuilder()
				.append(this.preparedDataFileName())
				.append(".crossjoins.stat.csv")
				.toString();
	}
	
	public void biGather() throws IOException {
		this.union.gather();
		this.intersection.gather();
		
		var sb = new StringBuilder(Workbench.statHeader);
		var sb_cj = new StringBuilder(Workbench.statHeader);
		sb.append("\n");
		sb_cj.append("\n");
		for(var join : this.joins) {
			join.gather(join.joined != null ? sb : sb_cj);
		}
		Files.writeString(this.resultFolder().resolve(this.joinResultFileName()), sb.toString());
		Files.writeString(this.resultFolder().resolve(this.crossjoinResultFileName()), sb_cj.toString());
	}
	
	public Table getPreparedData() {
		return this.preparedData;
	}
	
	public Map<String, Table> getSubdata(){
		return this.bidata;
	}
	
	/** union experiment */
	private BiExperiment union = BiExperiment.union(this);
	
	/** intersection experiment */
	private BiExperiment intersection = BiExperiment.intersection(this);
	
	/** Map of filename - attribute data used as left side of join experiments.
	 * Experiment class is responsible for creating the data files */
	protected abstract Map<String, List<Attribute<?>>> smallData();
	
	/** Projection experiment*/
	public final ProjectionExperiment projection = new ProjectionExperiment(this);
	
	private List<JoinExperiment<?>> joins = new LinkedList<>();
	
	/** Prepares join experiments 
	 * @throws TableRecordSchemaMismatch 
	 * @throws ClassNotInContextException 
	 * @throws ColumnOrderingNotInitializedException 
	 * @throws DuplicateAttributeNameException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws CsvValidationException */
	public void prepareJoins() throws CsvValidationException, ClassNotFoundException, IOException, DuplicateAttributeNameException, ColumnOrderingNotInitializedException, ClassNotInContextException, TableRecordSchemaMismatch {
		for(var sde : this.smallData().entrySet()) {
			for(Attribute<?> a : sde.getValue()) {
				var join = JoinExperiment.joinExperiment(this, sde.getKey(), a);
				join.prepare();
				this.joins.add(join);
			}
			
			var crossjoin = JoinExperiment.crossjoinExperiment(this, sde.getKey());
			crossjoin.prepare();
			this.joins.add(crossjoin);
		}
	}
	
	/** Prepares statistics for the join experiments */
	public void prepareJoinStatistics() throws IOException {
		for(var join : this.joins) {
			if(join.joined != null) {
				for(var slice : this.slices()) {
					var hist = new RankHistogram(slice);
					hist.gather(join.getSmallData());
					hist.writeFile(this.preparedDataHistFolder()
							.resolve(Workbench.rankHistFileName(join.smallDataId, slice)));
				}
				
				var hist = new AttributeHistogram<>(join.joined);
				hist.gather(join.getSmallData());
				Files.writeString(this.preparedDataHistFolder()
						.resolve(Workbench.histName(join.smallDataId, join.joined.name)), 
						JsonSerializer.instance().serialize(hist));
			}
		}
		
		for(var e : this.getSubdata().entrySet()) {
			var id = e.getKey();
			var data = e.getValue();
			
			for(var a : this.nominalAttributes()) {
				var hist = new AttributeHistogram<>(a);
				hist.gather(data);
				
				Files.writeString(this.preparedDataHistFolder()
						.resolve(Workbench.histName(this.subdataName(id), a.name)),
						JsonSerializer.instance().serialize(hist));
			}
		}
	}
	
	protected abstract List<Attribute<?>> projectionAttributes();
	
	protected abstract long seed();
	
	private void backupPreviousData() {
		var source = this.preparedDataFolder();
		if(Files.exists(source)) {
			var target = Workbench.backupDest(this.primaryDataPath(), source);
			try {
				Files.walkFileTree(source, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE,
				        new SimpleFileVisitor<Path>() {
		            @Override
		            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
		                throws IOException
		            {
		                Path targetdir = target.resolve(source.relativize(dir));
		                try {
		                    Files.copy(dir, targetdir); 
		                } catch (FileAlreadyExistsException e) {
		                     if (!Files.isDirectory(targetdir))
		                         throw e;
		                }
		                return FileVisitResult.CONTINUE;
		            }
		            @Override
		            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
		                throws IOException
		            {
		                Files.copy(file, target.resolve(source.relativize(file)));
		                Files.delete(file);
		                return FileVisitResult.CONTINUE;
		            }
		            
					@Override
					public FileVisitResult postVisitDirectory(Path dir, IOException exc) 
							throws IOException {
						Files.delete(dir);
						return FileVisitResult.CONTINUE;
					}
		        });
				System.out.println("Previous run backup complete");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	/** Executes the whole experiment 
	 * @throws DuplicateHeaderWriteException 
	 * @throws TableRecordSchemaMismatch 
	 * @throws ClassNotInContextException 
	 * @throws ColumnOrderingNotInitializedException 
	 * @throws DuplicateAttributeNameException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws CsvValidationException 
	 * @throws SchemaNotEqualException 
	 * @throws NotSubschemaException 
	 * @throws OnOperatornNotApplicableToSchemaException */
	public void experiment() throws CsvValidationException, ClassNotFoundException, IOException, DuplicateAttributeNameException, ColumnOrderingNotInitializedException, ClassNotInContextException, TableRecordSchemaMismatch, DuplicateHeaderWriteException, SchemaNotEqualException, NotSubschemaException, OnOperatornNotApplicableToSchemaException {
		var start = System.currentTimeMillis();	
		this.backupPreviousData();
		this.prepareData();
		System.out.println("Data prepared.");
		this.preparedDataStatistics();
		System.out.println("Basic statistics computed.");
		this.estimates();
		System.out.println("Estimates computed");
		this.queries();
		System.out.println("Sample queries computed.");
		this.gatherData();
		var end = System.currentTimeMillis();
		System.out.println("Finished, time: " + java.time.Duration.ofMillis(end - start).toString());
		
	}
	
	public String makeContract() {
		var atts = this.numericAttributes().stream()
				.map(a -> new EstimationExperimentContract.AttributeContract(
						a.name, 
						a.domain.getName(), 
						this.histSampleSize(a), 
						this.nConsideredValues().get(a).stream().findFirst().get(), 
						this.paretRatios().get(a).stream().findFirst().get(), 
						this.intervals(a).get(0), 
						this.similarity(a), 
						List.of()))
				.toList();
		
		var cnt = new EstimationExperimentContract(
				this.preparedDataFileName(),
				this.preparedDataFolder().toString(),
				this.slices().get(0),
				this.probes().get(0), 
				this.seed(),
				atts);
		
		return JsonSerializer.instance().serialize(cnt);
	}
	
	protected abstract Map<Attribute<Double>, ISimilarity<Double>> numericalSimilarityMap();
	
	protected abstract Collection<String> restrictionEstimationKnownConstantSignatures();
	protected abstract Collection<String> restrictionEstimationUnknownConstantSignatures();
}