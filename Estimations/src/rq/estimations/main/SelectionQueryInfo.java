package rq.estimations.main;

import java.nio.file.Path;

import rq.common.onOperators.Constant;
import rq.common.operators.LazySelection;
import rq.common.restrictions.Similar;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.Attribute;
import rq.common.table.LazyFacade;

public class SelectionQueryInfo {
	public final Path dataPath;
	public final Attribute attribute;
	public final double similarUntil;
	public final double constant;
	
	public SelectionQueryInfo(
			Path dataPath,
			Attribute attribute,
			double similarUntil,
			double constant) {
		this.dataPath = dataPath;
		this.attribute = attribute;
		this.similarUntil = similarUntil;
		this.constant = constant;
	}
	
	public LazySelection reconstruct() {
		return new LazySelection(new LazyFacade(ResourceLoader.instance().getOrLoadTable(this.dataPath)),
				new Similar(this.attribute, new Constant<Double>(this.constant),
						LinearSimilarity.doubleSimilarityUntil(this.similarUntil)));
	}
	
	public String dataFileName() {
		return this.dataPath.getFileName().toString();
	}
	
	public String queryFileNameBase() {
		return new StringBuilder()
				.append(this.dataFileName())
				.append(".att=")
				.append(this.attribute.name)
				.append(".sim=")
				.append(this.similarUntil)
				.append("con=")
				.append(this.constant)
				.toString();
	}
	
	public String queryFileName() {
		return new StringBuilder()
				.append(this.queryFileNameBase())
				.append(".csv")
				.toString();
	}
	
	@Override
	public String toString() {
		return this.queryFileNameBase();
	}
	
	@Override
	public int hashCode() {
		return new StringBuilder()
				.append(this.dataPath.hashCode())
				.append(this.attribute.hashCode())
				.append(Double.hashCode(this.similarUntil))
				.append(Double.hashCode(this.constant))
				.toString().hashCode();
	}
	
	@Override
	public boolean equals(Object other) {
		if(other instanceof SelectionQueryInfo) {
			var qi = (SelectionQueryInfo)other;
			return this.dataPath.equals(qi.dataPath)
					&& this.attribute.equals(qi.attribute)
					&& this.similarUntil == qi.similarUntil
					&& this.constant == qi.constant;
		}
		return false;
	}
}