package rq.estimations.main;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
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
import rq.common.onOperators.Constant;
import rq.common.operators.LazySelection;
import rq.common.operators.Selection;
import rq.common.restrictions.Similar;
import rq.common.statistic.AttributeHistogram;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SampledHistogram;
import rq.common.table.Attribute;
import rq.common.table.LazyFacade;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.exceptions.ColumnOrderingNotInitializedException;
import rq.files.exceptions.DuplicateHeaderWriteException;
import rq.files.io.TableReader;
import rq.files.io.TableWriter;

public abstract class Experiment {

	protected Experiment() {}
	
	/** Base folder for the experiments. Must contain primary data file */
	protected abstract Path folder();
	/** File containing the primary data. Primary data is unranked */
	protected abstract String primaryDataFileName();
	/** Name of the prepared data. Used to build folder structure */
	protected abstract String preparedDataName();
	
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
		
		var query = this.prepareDataQuery(primaryData);
		this.preparedData = query.eval();
		
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
		return this.preparedDataFolder().resolve("hist");
	}
	
	/** List of nominal attributes */
	protected abstract List<Attribute> numericAttributes();
	/** List of numerical attributes */
	protected abstract List<Attribute> nominalAttributes();
	
	/** Returns histogram sample size for given attribute */
	protected abstract double histSampleSize(Attribute a);
	
	/** Returns list of intervals used for given attribute */
	protected abstract List<Integer> intervals(Attribute a);
	
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
			var hist = new AttributeHistogram(a);
			hist.gather(this.preparedData);
			hist.writeFile(this.preparedDataHistFolder()
					.resolve(Workbench.histName(this.preparedDataFileName(), a.name)));
		}
		
		//Numerical attribure histograms
		for(var a : this.numericAttributes()) {
			var sampleSize = this.histSampleSize(a);
			var hist = new SampledHistogram(a, sampleSize);
			hist.gather(this.preparedData);
			hist.writeFile(this.preparedDataHistFolder()
					.resolve(Workbench.sampledHistName(this.preparedDataFileName(), a.name)));
			
			for(var i : this.intervals(a)) {
				var eqn = new EquinominalHistogram(a, i);
				eqn.gather(this.preparedData);
				eqn.writeFile(this.preparedDataHistFolder()
						.resolve(Workbench.eqnHistName(this.preparedDataFileName(), a.name, i)));
				
				var eqd = new EquidistantHistogram(a, i);
				eqd.gather(this.preparedData);
				eqd.writeFile(this.preparedDataHistFolder()
						.resolve(Workbench.eqdHistName(this.preparedDataFileName(), a.name, i)));
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
	protected abstract BiFunction<Object, Object, Double> similarity(Attribute a);
	
	/** Prefix of the estimation files */
	private String estFileNamePrefix(Attribute a) {
		return Workbench.estFileNamePrefix(
				this.preparedDataFileName(), 
				a.name);
	}
	
	/** List of tested number of probes in the experiment */
	protected abstract List<Integer> probes();
	
	/** List of stochastic estimation samples for given attribute */
	protected abstract List<Integer> estSamples(Attribute a);
	
	/** Computes the estimates */
	public void estimates() throws IOException, ClassNotFoundException {
		if(!Files.exists(this.preparedDataEstFolder())) {
			Files.createDirectory(this.preparedDataEstFolder());
		}
		
		this.biEstimates();
		this.numericEstimates();
		this.projection.estimate();
	}
	
	/** Computes the numeric estimates  */
	public void numericEstimates() throws IOException, ClassNotFoundException {	
		
		for(var a : this.numericAttributes()) {
			var fileNamePrefix = this.estFileNamePrefix(a);
			for(var slice : this.slices()) {
				var hist = RankHistogram.readFile(this.preparedDataHistFolder()
						.resolve(Workbench.rankHistFileName(this.preparedDataFileName(), slice)));
				
				//Interval estimates
				for(var i : this.intervals(a)) {
					var eqdHist = EquidistantHistogram.readFile(this.preparedDataHistFolder()
							.resolve(Workbench.eqdHistName(this.preparedDataFileName(), a.name, i)));
					
					Workbench.eqd(
							slice, 
							this.similarity(a),
							eqdHist,
							hist, 
							this.preparedDataEstFolder().toString(), 
							fileNamePrefix);
					
					Workbench.eqdC(
							slice, 
							this.similarity(a), 
							eqdHist, 
							hist, 
							this.preparedDataEstFolder().toString(), 
							fileNamePrefix);
					
					
					var eqnHist = EquinominalHistogram.readFile(this.preparedDataHistFolder()
							.resolve(Workbench.eqnHistName(this.preparedDataFileName(), a.name, i)));
					Workbench.eqn(
							slice, 
							this.similarity(a), 
							eqnHist, 
							hist, 
							this.preparedDataEstFolder().toString(), 
							fileNamePrefix);
					
					Workbench.eqnC(
							slice, 
							this.similarity(a), 
							eqnHist, 
							hist, 
							this.preparedDataEstFolder().toString(), 
							fileNamePrefix);
				}
				
				var sampledHist = SampledHistogram.readFile(this.preparedDataHistFolder()
						.resolve(Workbench.sampledHistName(this.preparedDataFileName(), a.name)));
				
				var selection = new Selection(this.preparedData,
						new Similar(a, new Constant<Double>(0d), this.similarity(a)));
				
				//Statistic estimates
				for(var probe : this.probes()) {
					Workbench.num(
							selection, 
							slice, 
							probe, 
							sampledHist, 
							hist, 
							this.preparedDataEstFolder().toString(), 
							fileNamePrefix);
					Workbench.numP(
							selection, 
							slice, 
							probe, 
							sampledHist, 
							hist, 
							this.preparedDataEstFolder().toString(), 
							fileNamePrefix);
					for(var sample : this.estSamples(a)) {
						Workbench.numS(
								selection, 
								slice, 
								probe, 
								sample, 
								sampledHist, 
								hist, 
								this.preparedDataEstFolder().toString(), 
								fileNamePrefix);
						Workbench.numPS(
								selection, 
								slice, 
								probe, 
								sample, 
								sampledHist, 
								hist, 
								this.preparedDataEstFolder().toString(), 
								fileNamePrefix);
					}
				}
			}
		}
	}
	
	/** List of values to be queried as sample queries for given attribute*/
	protected abstract List<Object> queryValues(Attribute a);
	
	/** Folder where query results are preserved */
	Path queryFolder() {
		return this.preparedDataFolder().resolve("queries");
	}
	
	/** Name of the query file */
	private String queryFileName(Attribute a, Object v) {
		return new StringBuilder()
				.append(this.primaryDataFileName())
				.append(".")
				.append(a.name)
				.append(".")
				.append(v.toString())
				.append(".csv")
				.toString();
	}
	
//	/** Query file */
//	private Path queryFile(Attribute a, Object v) {
//		return this.queryFolder().resolve(this.queryFileName(a, v));
//	}
	
	/** All attributes measured by this experiment */
	private Set<Attribute> allMeasuredAttributes(){
		var attrs = new HashSet<Attribute>(this.numericAttributes());
		attrs.addAll(this.nominalAttributes());
		return attrs;
	}
	
	private Map<Attribute, List<Double>> _sampleQueryValues = new LinkedHashMap<Attribute, List<Double>>();
	private Random rand = new Random(this.seed());
	
	private List<Double> sampleQueryValues(Attribute a) throws ClassNotFoundException, IOException{
		var vls = this._sampleQueryValues.get(a); 
		if(vls == null) {
			var sHist = this.sampledHist(a);
			var min = sHist.min();
			var max = sHist.max();
			vls =  Stream.generate(new Supplier<Double>() {
	
				@Override
				public Double get() {
					return min + rand.nextDouble() * max;
				}
				
			}).limit(this.SAMPLE_QUERY_COUNT).collect(Collectors.toList());
			this._sampleQueryValues.put(a, vls);
		}
		return vls;
	}
	
	public final long SAMPLE_QUERY_COUNT = 200;
	
	private Path sampledHistFile(Attribute a) {
		return this.preparedDataHistFolder().resolve(
				Workbench.sampledHistName(this.preparedDataFileName(), a.name));
	}
	
	public SampledHistogram sampledHist(Attribute a) throws ClassNotFoundException, IOException {
		return SampledHistogram.readFile(this.sampledHistFile(a));
	}
	
	/** Computes the queries  
	 * @throws DuplicateAttributeNameException 
	 * @throws NotSubschemaException 
	 * @throws OnOperatornNotApplicableToSchemaException 
	 * @throws ClassNotFoundException */
	public void queries() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException, SchemaNotEqualException, NotSubschemaException, DuplicateAttributeNameException, OnOperatornNotApplicableToSchemaException, ClassNotFoundException {
		if(!Files.exists(this.queryFolder())) {
			Files.createDirectory(this.queryFolder());
		}
		
		for(var a : this.numericAttributes()) {			
			var values = this.sampleQueryValues(a);
			if(values != null) {
				for(var v : values) {					
					var selection = new LazySelection(
							new LazyFacade(this.preparedData),
							new Similar(a, new Constant<Object>(v), this.similarity(a)));
					
					var hists = this.slices().stream().map(s -> new RankHistogram(s)).collect(Collectors.toList());
					
					var start = System.currentTimeMillis();
					var record = selection.next();
					//Skip if empty
					if(record == null) {
						continue;
					}
					
					while(record != null) {
						for(var hist : hists) {
							hist.addRank(record.rank);
						}
						record = selection.next();
					}
					var end = System.currentTimeMillis();
					
					for(var hist : hists) {
						hist.writeFile(this.preparedDataHistFolder()
								.resolve(Workbench.rankHistFileName(this.queryFileName(a, v), hist.getSlices().size())));
					}
					
					System.out.println(this.queryFileName(a, v) + " duration " + Duration.ofMillis(end - start).toString());
				}
			}
		}
		
		this.biQueries();
		this.projection.query();
	}
	
	/** Gathers all estimation for given attribute and slice */
	private Map<String, RankHistogram> gatherEst(Attribute a, int slice) throws IOException{
		var ret = new LinkedHashMap<String, RankHistogram>();
		var fileNamePrefix = this.estFileNamePrefix(a);
		
		for(var i : this.intervals(a)) {
			var eqn = Workbench.eqnEstName(fileNamePrefix, slice, i);
			ret.put(eqn, RankHistogram.readFile(this.preparedDataEstFolder()
					.resolve(eqn)));
			
			var eqnC = Workbench.eqnCEstName(fileNamePrefix, slice, i);
			ret.put(eqnC, RankHistogram.readFile(this.preparedDataEstFolder()
					.resolve(eqnC)));
			
			var eqd = Workbench.eqdEstName(fileNamePrefix, slice, i);
			ret.put(eqd, RankHistogram.readFile(this.preparedDataEstFolder()
					.resolve(eqd)));
			
			var eqdC = Workbench.eqdCEstName(fileNamePrefix, slice, i);
			ret.put(eqdC, RankHistogram.readFile(this.preparedDataEstFolder()
					.resolve(eqdC)));
		}
		
		for(var probe : this.probes()) {
			var num = Workbench.numEstName(fileNamePrefix, slice, probe);
			ret.put(num, 
					RankHistogram.readFile(this.preparedDataEstFolder()
							.resolve(num)));
			var numP = Workbench.numPEstName(fileNamePrefix, slice, probe);
			ret.put(numP, 
					RankHistogram.readFile(this.preparedDataEstFolder()
							.resolve(numP)));
			for(var sample : this.estSamples(a)) {
				var numS = Workbench.numSEstName(fileNamePrefix, slice, probe, sample);
				ret.put(numS, 
						RankHistogram.readFile(this.preparedDataEstFolder()
								.resolve(numS)));
				var numPS = Workbench.numPSEstName(fileNamePrefix, slice, probe, sample);
				ret.put(numPS, 
						RankHistogram.readFile(this.preparedDataEstFolder()
								.resolve(numPS)));
			}
		}
		
		return ret;
	}
	
	/**Gathers all sample queries for given attribute and slice 
	 * @throws ClassNotFoundException */
	private Map<String, RankHistogram> gatherQueries(Attribute a, int slice) throws IOException, ClassNotFoundException{
		var ret = new LinkedHashMap<String, RankHistogram>();
		
		for(var v : this.sampleQueryValues(a)) {
			var name = Workbench.rankHistFileName(this.queryFileName(a, v), slice);
			var path = this.preparedDataHistFolder()
					.resolve(name);
			
			if(Files.exists(path)) {
				ret.put(name, RankHistogram.readFile(path));
			}
		}
		
		return ret;
	}
	
	/** Folder with results */
	Path resultFolder() {
		return this.preparedDataFolder().resolve("result");
	}
	
	/** Name of the result file for given attribute and slice */
	private String resultFileName() {
		return new StringBuilder()
				.append(this.preparedDataFileName())
				.append(".restrictions.stat.csv")
				.toString();
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
		var tableSize = this.preparedData.size();
		
		if(!Files.exists(this.resultFolder())) {
			Files.createDirectory(this.resultFolder());
		}
		
		var sb = new StringBuilder(Workbench.statHeader);
		sb.append(", estimation\n");
		for(var a : this.numericAttributes()) {
			var estPrefix = this.estFileNamePrefix(a).length();
			
			for(var slice : this.slices()) {
				var ests = this.gatherEst(a, slice);
				var queries = this.gatherQueries(a, slice);
				
				for(var est : ests.entrySet()) {
					for(var query : queries.entrySet()){
						var estName = est.getKey().substring(estPrefix + 1);
						estName = estName.substring(0, estName.indexOf('.'));
						
						sb.append(Workbench.statLine(
								est.getValue(),
								est.getKey(),
								query.getValue(),
								query.getKey(),
								estName,
								tableSize));
						sb.append("\n");
					}
				}
			}
		}
		Files.writeString(this.resultFolder().resolve(this.resultFileName()), sb.toString());
		
		this.biGather();
		this.projection.gather();
	}
	
	/** Prepare data for biexperiments */
	protected abstract Map<String, TabularExpression> prepareSubDataQueries(Table preparedData);
	
	/** sub data file name */
	String subdataName(String identifier) {
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
	protected abstract Map<String, List<Attribute>> smallData();
	
	/** Projection experiment*/
	public final ProjectionExperiment projection = new ProjectionExperiment(this);
	
	private List<JoinExperiment> joins = new LinkedList<JoinExperiment>();
	
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
			for(Attribute a : sde.getValue()) {
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
				
				var hist = new AttributeHistogram(join.joined);
				hist.gather(join.getSmallData());
				hist.writeFile(this.preparedDataHistFolder()
						.resolve(Workbench.histName(join.smallDataId, join.joined.name)));
			}
		}
		
		for(var e : this.getSubdata().entrySet()) {
			var id = e.getKey();
			var data = e.getValue();
			
			for(var a : this.nominalAttributes()) {
				var hist = new AttributeHistogram(a);
				hist.gather(data);
				hist.writeFile(this.preparedDataHistFolder()
						.resolve(Workbench.histName(this.subdataName(id), a.name)));
			}
		}
	}
	
	protected abstract List<Attribute> projectionAttributes();
	
	protected abstract long seed();
	
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
}
