package rq.estimations.framework;

import java.nio.file.Path;

import rq.common.onOperators.Constant;
import rq.common.operators.LazySelection;
import rq.common.restrictions.Similar;
import rq.common.similarities.ISimilarity;
import rq.common.table.Attribute;
import rq.common.table.LazyFacade;
import rq.common.util.NameGenerator;

public class SelectionQueryInfo<T> {
	public final Path dataPath;
	public final Attribute<T> attribute;
	public final ISimilarity<T> similarity;
	public final T constant;
	public final String uid;
	
	public SelectionQueryInfo(
			Path dataPath,
			Attribute<T> attribute,
			ISimilarity<T> similarity,
			T constant) {
		this.dataPath = dataPath;
		this.attribute = attribute;
		this.similarity = similarity;
		this.constant = constant;
		this.uid = NameGenerator.instance().next(".qry=");
	}
	
	public LazySelection reconstruct() {
		return new LazySelection(new LazyFacade(ResourceLoader.instance().getOrLoadTable(this.dataPath)),
				new Similar<>(this.attribute, new Constant<T>(this.constant),
						this.similarity));
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
				.append(this.similarity.signature())
				.append(".con=")
				.append(this.constant)
				.append(this.uid)
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
				.append(this.similarity.hashCode())
				.append(this.constant.hashCode())
				.append(this.uid.hashCode())
				.toString().hashCode();
	}
	
	@Override
	public boolean equals(Object other) {
		if(other instanceof SelectionQueryInfo) {
			var qi = (SelectionQueryInfo<?>)other;
			return this.dataPath.equals(qi.dataPath)
					&& this.attribute.equals(qi.attribute)
					&& this.similarity.equals(qi.similarity)
					&& this.constant == qi.constant
					&& this.uid == qi.uid;
		}
		return false;
	}
}