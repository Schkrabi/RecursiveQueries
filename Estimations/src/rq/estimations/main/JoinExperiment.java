package rq.estimations.main;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.stream.Collectors;

import com.opencsv.exceptions.CsvValidationException;

import rq.common.estimations.EstimateCrossJoin;
import rq.common.estimations.EstimateJoinNominal;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.SchemaNotEqualException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.io.contexts.ClassNotInContextException;
import rq.common.latices.LaticeFactory;
import rq.common.operators.LazyJoin;
import rq.common.statistic.AttributeHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.table.Attribute;
import rq.common.table.LazyFacade;
import rq.files.exceptions.ColumnOrderingNotInitializedException;
import rq.files.exceptions.DuplicateHeaderWriteException;
import rq.files.io.TableReader;

public abstract class JoinExperiment {

	public final Experiment parent;
	/** Small data for the left side of the join*/
	protected Table smallData = null;
	protected RankHistogram smallDataHist = null;
	/** Name of the small data */
	public final String smallDataId;
	/** Attribute to join over */
	public final Attribute joined;
	
	private JoinExperiment(Experiment parent, String smallDataId, Attribute joined) {
		this.parent = parent;
		this.smallDataId = smallDataId;
		this.joined = joined;
	}
	
	public Table getSmallData() {
		return this.smallData;
	}

	/** Name of the experiment */
	protected abstract String name();
	
	public void prepare() throws IOException, CsvValidationException, ClassNotFoundException, DuplicateAttributeNameException, ColumnOrderingNotInitializedException, ClassNotInContextException, TableRecordSchemaMismatch {
		var tr = TableReader.open(Files.newInputStream(this.parent.preparedDataFolder().resolve(this.smallDataId)));
		this.smallData = tr.read();
		tr.close();
	}
	
	/** estimation file name*/
	public String estFileName(String id, int slice) {
		return new StringBuilder()
				.append(parent.preparedDataFileName())
				.append(".")
				.append(this.name())
				.append(".")
				.append(this.smallDataId)
				.append(".")
				.append(id)
				.append(".")
				.append(slice)
				.append(".est")
				.toString();			
	}
	
	/** Provides the estimate for given slice 
	 * @throws IOException 
	 * @throws ClassNotFoundException */
	protected abstract RankHistogram provideEstimate(int slice, RankHistogram hist, String id) throws ClassNotFoundException, IOException;
	
	/** Computes and saves estimates */
	public void estimate() throws IOException, ClassNotFoundException {
		for(var slice : this.parent.slices()) {
			this.smallDataHist = RankHistogram.readFile(this.parent.preparedDataHistFolder()
					.resolve(Workbench.rankHistFileName(this.smallDataId, slice)));
			for(var right : this.parent.getSubdata().entrySet()) {
				var id = this.parent.subdataName(right.getKey());
				var hist = RankHistogram.readFile(this.parent.preparedDataHistFolder()
						.resolve(Workbench.rankHistFileName(id, slice)));
				
				var est = this.provideEstimate(slice, hist, id);
				est.writeFile(this.parent.preparedDataEstFolder()
						.resolve(this.estFileName(right.getKey(), slice)));
			}
		}
	}
	
	/** Name of the query file */
	public String queryFileName(String id) {
		return new StringBuilder()
				.append(this.parent.preparedDataFileName())
				.append(".")
				.append(this.name())
				.append(".")
				.append(this.smallDataId)
				.append(".")
				.append(id)
				.append(this.joined != null ? "." : "")
				.append(this.joined != null ? this.joined.name : "")
				.append(".csv")
				.toString();
	}
	
	/** Provides the experiment sample query  
	 * @throws OnOperatornNotApplicableToSchemaException */
	protected abstract LazyExpression doQuery(Table t) throws SchemaNotEqualException, OnOperatornNotApplicableToSchemaException;
	
	/** computes the sample queries  
	 * @throws OnOperatornNotApplicableToSchemaException */
	public void query() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException, SchemaNotEqualException, OnOperatornNotApplicableToSchemaException {
		for(var right : this.parent.getSubdata().entrySet()) {
			var rid = right.getKey();
			var rTable = right.getValue();
			
			var hists = this.parent.slices().stream().map(slice -> new RankHistogram(slice))
					.collect(Collectors.toList());
			
			var q = this.doQuery(rTable);
			
			var start = System.currentTimeMillis();
			var record = q.next();
			while(record != null) {
				for(var hist : hists) {
					hist.addRank(record.rank);
				}
				record = q.next();
			}
			var end = System.currentTimeMillis();
			
			for(var hist : hists) {
				hist.writeFile(this.parent.preparedDataHistFolder()
						.resolve(Workbench.rankHistFileName(this.queryFileName(rid), hist.getSlices().size())));
			}
			System.out.println(this.queryFileName(rid) + " duration " + Duration.ofMillis(end - start).toString());
		}
	}
	
	/** result file name */
	public String resultFileName() {
		return new StringBuilder()
				.append(this.parent.preparedDataFileName())
				.append(".")
				.append(this.name())
				.append(".")
				.append(this.smallDataId)
				.append(".stat.csv")
				.toString();
	}
	
	/** Maximum size of the result */
	protected int maxResultSize(Table t) {
		return this.smallData.size() * t.size();
	}
	
	/** Gathers result */
	public void gather(StringBuilder sb) throws IOException {
		for(var re : this.parent.getSubdata().entrySet()) {
			var right = re.getKey();
			var size = this.maxResultSize(re.getValue());
			
			for(var slice : this.parent.slices()) {
				var qName = Workbench.rankHistFileName(this.queryFileName(right), slice);
				var qHist = RankHistogram.readFile(this.parent.preparedDataHistFolder()
						.resolve(qName));
				var eName = this.estFileName(right, slice);
				var eHist = RankHistogram.readFile(this.parent.preparedDataEstFolder()
						.resolve(eName));
				sb.append(Workbench.statLine(eHist, eName, qHist, qName, size));
				sb.append("\n");
			}
		}
	}
	
	public static JoinExperiment joinExperiment(Experiment parent, String fileName, Attribute joined) {
		return new JoinExperiment(parent, fileName, joined) {

			@Override
			protected String name() {
				return "join";
			}

			@Override
			protected RankHistogram provideEstimate(int slice, RankHistogram hist, String id)
					throws ClassNotFoundException, IOException {
				var lah = AttributeHistogram.readFile(this.parent.preparedDataHistFolder()
						.resolve(Workbench.histName(this.smallDataId, joined.name)));
				var rah = AttributeHistogram.readFile(this.parent.preparedDataHistFolder()
						.resolve(Workbench.histName(id, joined.name)));
				return EstimateJoinNominal.estimate(
						this.smallDataHist, 
						hist, 
						lah, 
						rah, 
						LaticeFactory.instance().getProduct());
			}

			@Override
			protected LazyExpression doQuery(Table t)
					throws SchemaNotEqualException, OnOperatornNotApplicableToSchemaException  {
				return LazyJoin.factory(
						new LazyFacade(this.smallData), 
						new LazyFacade(t), 
						new rq.common.onOperators.OnEquals(this.joined, this.joined));
			}
			
		};
	}
	
	public static JoinExperiment crossjoinExperiment(Experiment parent, String filename) {
		return new JoinExperiment(parent, filename, null) {

			@Override
			protected String name() {
				return "crossjoin";
			}

			@Override
			protected RankHistogram provideEstimate(int slice, RankHistogram hist, String id)
					throws ClassNotFoundException, IOException {
				return EstimateCrossJoin.estimate(this.smallDataHist, hist);
			}

			@Override
			protected LazyExpression doQuery(Table t)
					throws SchemaNotEqualException, OnOperatornNotApplicableToSchemaException {
				return LazyJoin.crossJoin(new LazyFacade(this.smallData), new LazyFacade(t));
			}
			
		};
	}

}
