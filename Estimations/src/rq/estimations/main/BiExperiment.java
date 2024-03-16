package rq.estimations.main;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.stream.Collectors;

import rq.common.estimations.EstimateUnion;
import rq.common.estimations.IntersectionEstimation;
import rq.common.exceptions.SchemaNotEqualException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.operators.LazyIntersection;
import rq.common.operators.LazyUnion;
import rq.common.statistic.RankHistogram;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.exceptions.DuplicateHeaderWriteException;

public abstract class BiExperiment {
	
	protected final Experiment parent;
	
	public BiExperiment(Experiment parent) {
		this.parent = parent;
	}

	/** Name of the experiment */
	protected abstract String name();
	
	/** estimation file name*/
	public String estFileName(String lid, String rid, int slice) {
		return new StringBuilder()
				.append(parent.preparedDataFileName())
				.append(".")
				.append(this.name())
				.append(".")
				.append(lid)
				.append(".")
				.append(rid)
				.append(".")
				.append(slice)
				.append(".est")
				.toString();			
	}
	
	/** Provides the estimate for given slice 
	 * @throws IOException 
	 * @throws ClassNotFoundException */
	protected abstract RankHistogram provideEstimate(int slice, RankHistogram left, String lid, RankHistogram right, String rid) throws ClassNotFoundException, IOException;
	
	/** Computes and saves estimates */
	public void estimate() throws IOException, ClassNotFoundException {
		for(var slice : this.parent.slices()) {
			for(var left : this.parent.getSubdata().entrySet()) {
				var lid = this.parent.subdataName(left.getKey());
				var lhist = RankHistogram.readFile(this.parent.preparedDataHistFolder()
						.resolve(Workbench.rankHistFileName(lid, slice)));
				
				for(var right : this.parent.getSubdata().entrySet()) {
					if(left.equals(right)) {
						continue;
					}
					
					var rid = this.parent.subdataName(right.getKey());
					var rhist = RankHistogram.readFile(this.parent.preparedDataHistFolder()
							.resolve(Workbench.rankHistFileName(rid, slice)));
					
					var est = this.provideEstimate(slice, lhist, lid, rhist, rid);
					est.writeFile(this.parent.preparedDataEstFolder()
							.resolve(this.estFileName(left.getKey(), right.getKey(), slice)));
				}
			}
		}
	}
	
	/** Name of the query file */
	public String queryFileName(String lid, String rid) {
		return new StringBuilder()
				.append(this.parent.preparedDataFileName())
				.append(".")
				.append(this.name())
				.append(".")
				.append(lid)
				.append(".")
				.append(rid)
				.append(".csv")
				.toString();
	}
	
	/** Provides the experiment sample query  */
	protected abstract LazyExpression doQuery(Table left, Table right) throws SchemaNotEqualException;
	
	/** computes the sample queries  */
	public void query() throws IOException, ClassNotInContextException, DuplicateHeaderWriteException, SchemaNotEqualException {
		for(var left : this.parent.getSubdata().entrySet()) {
			for(var right : this.parent.getSubdata().entrySet()) {
				if(left.equals(right)) {
					continue;
				}
				
				var lid = left.getKey();
				var lTable = right.getValue();
				
				var rid = right.getKey();
				var rTable = right.getValue();
				
				var q = this.doQuery(lTable, rTable);
				var hists = this.parent.slices().stream().map(slice -> new RankHistogram(slice))
						.collect(Collectors.toList());
				
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
							.resolve(Workbench.rankHistFileName(this.queryFileName(lid, rid), hist.getSlices().size())));
				}
				System.out.println(this.queryFileName(lid, rid) + " duration " + Duration.ofMillis(end - start).toString());
			}
		}
	}
	
	/** result file name */
	public String resultFileName() {
		return new StringBuilder()
				.append(this.parent.preparedDataFileName())
				.append(".")
				.append(this.name())
				.append(".stat.csv")
				.toString();
	}
	
	/** All rows that estimate took into account */
	protected abstract int estimateDataSize(Table left, Table right);
	
	/** Gathers result */
	public void gather() throws IOException {
		var sb = new StringBuilder();
		sb.append(Workbench.statHeader);
		sb.append("\n");
		for(var le : this.parent.getSubdata().entrySet()) {
			for(var re : this.parent.getSubdata().entrySet()) {
				var left = le.getKey();
				var right = re.getKey();
				
				if(left.equals(right)) {
					continue;
				}
				var size = this.estimateDataSize(le.getValue(), re.getValue());
				
				for(var slice : this.parent.slices()) {
					var qName = Workbench.rankHistFileName(this.queryFileName(left, right), slice);
					var qHist = RankHistogram.readFile(this.parent.preparedDataHistFolder()
							.resolve(qName));
					var eName = this.estFileName(left, right, slice);
					var eHist = RankHistogram.readFile(this.parent.preparedDataEstFolder()
							.resolve(eName));
					sb.append(Workbench.statLine(eHist, eName, qHist, qName, size));
					sb.append("\n");
				}
			}
		}
		Files.writeString(this.parent.resultFolder().resolve(this.resultFileName()),
				sb.toString());
	}
	
	/** Union experiment */
	public static BiExperiment union(Experiment parent) {
		return new BiExperiment(parent) {

			@Override
			protected String name() {
				return "union";
			}

			@Override
			protected RankHistogram provideEstimate(int slice, RankHistogram left, String lid, RankHistogram right, String rid) {
				return EstimateUnion.estimate(left, right);
			}

			@Override
			protected LazyExpression doQuery(Table left, Table right) throws SchemaNotEqualException {
				return LazyUnion.factory(left, right);
			}

			@Override
			protected int estimateDataSize(Table left, Table right) {
				return left.size() + right.size();
			}
			
		};
	}
	
	/** Intersection experiment */
	public static BiExperiment intersection(Experiment parent) {
		return new BiExperiment(parent){

			@Override
			protected String name() {
				return "intersection";
			}

			@Override
			protected RankHistogram provideEstimate(int slice, RankHistogram left, String lid, RankHistogram right, String rid) {
				return IntersectionEstimation.estimate(left, right);
			}

			@Override
			protected LazyExpression doQuery(Table left, Table right) {
				try {
					return LazyIntersection.factory(left, right);
				} catch (SchemaNotEqualException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			protected int estimateDataSize(Table left, Table right) {
				return left.size() + right.size();
			}
			
		};
	}
}
