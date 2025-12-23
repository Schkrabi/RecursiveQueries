package rq.estimations.main;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import rq.common.operators.LazySelection;
import rq.common.similarities.LinearSimilarity;
import rq.common.statistic.IGeneratorProvider;
import rq.common.table.Attribute;

public abstract class RestrictionQueries {
	//Params
	public final Path dataPath;
	public final Attribute attribute;
	public final int count;	
	public final double similarUntil;
	public final Random rand;
	
	//Derived
	public final BiFunction<Object, Object, Double> similarity;
	
	//Computed
	private Collection<SelectionQueryInfo> _infos = null;
	private Map<SelectionQueryInfo, LazySelection> _selects = null;
	private List<Double> _values = null;
	
	public RestrictionQueries(
			Path dataPath,
			Attribute attribute,
			int count,
			double similarUntil) {
		this.dataPath = dataPath;
		this.attribute = attribute;
		this.count = count;
		this.similarUntil = similarUntil;
		
		this.similarity = LinearSimilarity.doubleSimilarityUntil(similarUntil);
		this.rand = new Random(System.currentTimeMillis());
	}
	
	public RestrictionQueries(
			Path dataPath,
			Attribute attribute,
			int count,
			double similarUntil,
			Random rand) {
		this.dataPath = dataPath;
		this.attribute = attribute;
		this.count = count;
		this.similarUntil = similarUntil;
		
		this.similarity = LinearSimilarity.doubleSimilarityUntil(similarUntil);
		this.rand = rand;
	}
	
//	public Attribute getAttribute() {
//		return this.sHist.observed;
//	}
	
	/** Generates new set of values for the queries*/
	protected abstract List<Double> generateValues();
	public Collection<Double> getValues(){
		if(this._values == null) {
			this._values = this.generateValues();
		}
		return this._values;
	}
	
	public Collection<SelectionQueryInfo> getInfos(){
		if(this._infos == null) {
			this._infos = this.getValues().stream()
					.map(v -> new SelectionQueryInfo(
							this.dataPath,
							this.attribute,
							this.similarUntil,
							v))
					.collect(Collectors.toList());
		}
		return this._infos;
	}
	
	public Map<SelectionQueryInfo, LazySelection> getSelections(){
		if(this._selects == null) {
			this._selects = new HashMap<>();
			for(var i : this.getInfos()) {
				this._selects.put(i, i.reconstruct());
			}
		}
		return this._selects;
	}
	
	public static final class Paret extends RestrictionQueries {

		public final IGeneratorProvider hist;
		
		public Paret(
				Path dataPath,
				Attribute attribute,
				int count,
				double similarUntil,
				IGeneratorProvider hist) {
			super(dataPath, attribute, count, similarUntil);
			this.hist = hist;
		}
		
		public Paret(
				Path dataPath,
				Attribute attribute,
				int count,
				double similarUntil,
				IGeneratorProvider hist,
				Random rand) {
			super(dataPath, attribute, count, similarUntil, rand);
			this.hist = hist;
		}

		@Override
		protected List<Double> generateValues() {
			var vls = this.hist.generator(this.rand).doubles()
					.limit(this.count)
					.boxed().collect(Collectors.toList());
			return vls;
		}		
	}
	
	public static final class Uniform extends RestrictionQueries {
		
		public final double min;
		public final double max;
		
		public Uniform(
				Path dataPath,
				Attribute attribute,
				int count,
				double similarUntil) {
			super(dataPath, attribute, count, similarUntil);
			var hist = ResourceLoader.instance().getOrLoadSampledHistogram(dataPath, attribute);
			this.min = hist.min();
			this.max = hist.max();
		}
		
		public Uniform(
				Path dataPath,
				Attribute attribute,
				int count,
				double similarUntil,
				Random rand) {
			super(dataPath, attribute, count, similarUntil, rand);
			var hist = ResourceLoader.instance().getOrLoadSampledHistogram(dataPath, attribute);
			this.min = hist.min();
			this.max = hist.max();
		}

		@Override
		protected List<Double> generateValues() {
			//Note: Query values are picked uniformly from the effective domain
			var vls =  Stream.generate(new Supplier<Double>() {

				@Override
				public Double get() {
					return min + rand.nextDouble() * max;
				}
				
			}).limit(this.count).collect(Collectors.toList());
			return vls;
		}
	}
	
	/** Constant set values*/
	public static final class FromValues extends RestrictionQueries {

		private Collection<Double> values;
		
		public FromValues(Path dataPath, Attribute attribute, double similarUntil, Collection<Double> values) {
			super(dataPath, attribute, values.size(), similarUntil);
			this.values = values;
		}

		@Override
		protected List<Double> generateValues() {
			return new ArrayList<>(this.values);
		}
		
	}
}
