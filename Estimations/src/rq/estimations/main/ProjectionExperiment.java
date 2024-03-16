package rq.estimations.main;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import rq.common.estimations.EstimateProjectionNominal;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.NotSubschemaException;
import rq.common.exceptions.SchemaNotEqualException;
import rq.common.interfaces.TabularExpression;
import rq.common.operators.Projection;
import rq.common.statistic.AttributeHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.table.Attribute;
import rq.common.table.FileMappedTable;
import rq.common.table.Schema;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.exceptions.DuplicateHeaderWriteException;

public class ProjectionExperiment {

	protected final Experiment parent;
	
	public ProjectionExperiment(Experiment parent) {
		this.parent = parent;
	}
	
	/** estimation file name*/
	public String estFileName(int slice) {
		return new StringBuilder()
				.append(parent.preparedDataFileName())
				.append(".")
				.append("projection")
				.append(".")
				.append(slice)
				.append(".est")
				.toString();			
	}
	
	/** Returns value counts of the data */
	private List<Integer> valueCounts() throws ClassNotFoundException, IOException{
		var rslt = new LinkedList<Integer>();
		for(var a : this.parent.projectionAttributes()) {
			var hist = AttributeHistogram.readFile( 
					this.parent.preparedDataHistFolder().resolve(Workbench.histName(this.parent.preparedDataFileName(), a.name)));
			
			rslt.add(hist.distinctValues());
		}
		return rslt;
	}
	
	/** Computes and saves estimates */
	public void estimate() throws IOException, ClassNotFoundException {
		var valueCounts = this.valueCounts();
		
		for(var slice : this.parent.slices()) {
			var hist = RankHistogram.readFile(this.parent.preparedDataHistFolder()
					.resolve(Workbench.rankHistFileName(this.parent.preparedDataFileName(), slice)));
			
			var est = EstimateProjectionNominal.estimate(hist, valueCounts);
			est.writeFile(this.parent.preparedDataEstFolder()
					.resolve(this.estFileName(slice)));
		}
	}
	
	/** Name of the query file */
	public String queryFileName(Attribute excluded) {
		return new StringBuilder()
				.append(this.parent.preparedDataFileName())
				.append(".")
				.append("projection")
				.append(".")
				.append(excluded.name)
				.append(".csv")
				.toString();
	}
	
	/** Prepares the projection with excluded attribute */
	private TabularExpression prepareQuery(Attribute excluded) throws NotSubschemaException, DuplicateAttributeNameException {
		return Projection.factory(
				this.parent.getPreparedData(), 
				Schema.factory(
					this.parent.getPreparedData().schema().stream()
						.filter(a -> !a.equals(excluded))
						.collect(Collectors.toList())),
				(s, i) -> {
					try {
						return FileMappedTable.factory(s, i);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				});
	}
	
	/** computes the sample queries  */
	public void query() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException, SchemaNotEqualException, NotSubschemaException, DuplicateAttributeNameException {
		for(var a : this.parent.projectionAttributes()) {
			var hists = this.parent.slices().stream().map(slice -> new RankHistogram(slice))
					.collect(Collectors.toList());
			
			var q = this.prepareQuery(a);	
			var start = System.currentTimeMillis();
			var table = q.eval();
			var end = System.currentTimeMillis();
			
			for(var hist : hists) {
				hist.gather(table);
				hist.writeFile(this.parent.preparedDataHistFolder()
						.resolve(Workbench.rankHistFileName(this.queryFileName(a), hist.getSlices().size())));
			}
			
			if(table instanceof FileMappedTable) {
				((FileMappedTable)table).close();
			}
			
			System.out.println(this.queryFileName(a) + " duration " + Duration.ofMillis(end - start).toString());
		}
	}
	
	/** result file name */
	public String resultFileName() {
		return new StringBuilder()
				.append(this.parent.preparedDataFileName())
				.append(".")
				.append("projection")
				.append(".")
				.append(".stat.csv")
				.toString();
	}
	
	/** Maximum size of the result */
	protected int maxResultSize() {
		return this.parent.getPreparedData().size();
	}
	
	/** Gathers result */
	public void gather() throws IOException {
		var sb = new StringBuilder();
		var size = this.maxResultSize();
		sb.append(Workbench.statHeader);
		sb.append("\n");
		
		
		for(var slice : this.parent.slices()) {
			var estName = this.estFileName(slice);
			var est = RankHistogram.readFile(this.parent.preparedDataEstFolder()
					.resolve(estName));
			
			for(var a : this.parent.projectionAttributes()) {
				var histName = Workbench.rankHistFileName(this.queryFileName(a), slice);
				var hist = RankHistogram.readFile(this.parent.preparedDataHistFolder()
						.resolve(histName));
				
				sb.append(Workbench.statLine(est, estName, hist, histName, size));
				sb.append("\n");				
			}
		}
		
		Files.writeString(this.parent.resultFolder().resolve(this.resultFileName()),
				sb.toString());
	}
}
