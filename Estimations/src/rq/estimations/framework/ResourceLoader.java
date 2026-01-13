package rq.estimations.framework;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import rq.common.interfaces.Table;
import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.MostCommonValues;
import rq.common.statistic.SampledHistogram;
import rq.common.table.Attribute;
import rq.estimations.main.Workbench;
import rq.files.io.TableReader;

/** Holds and caches all resources for experiments*/
public class ResourceLoader {

	private static ResourceLoader singleton = null;
	public static ResourceLoader instance() {
		if(singleton == null) {
			singleton = new ResourceLoader();
		}
		return singleton;
	}
	private ResourceLoader() {
	}

	private Map<Path, Table> cachedTables = new HashMap<Path, Table>();
	
	public Table getOrLoadTable(Path path) {
		var t = cachedTables.get(path);
		if(t == null) {
			try {
				var tr = TableReader.open(path);
				t = tr.read();
				cachedTables.put(path, t);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return t;
	}
	
	private Map<Path, SampledHistogramCache> _sampledHistCache = new HashMap<>();
	public <T extends Number> SampledHistogram<T> getOrLoadSampledHistogram(Path table, Attribute<T> a) {
		var c = _sampledHistCache.get(table);
		if(c == null) {
			c = new SampledHistogramCache(table);
			this._sampledHistCache.put(table, c);
		}
		return c.getOrLoad(a);
	}
	
	private static class SampledHistogramCache {
		private final Map<Attribute<? extends Number>, SampledHistogram<? extends Number>> _cache = new HashMap<>();
		private final Path histFolder;
		private final String dataFileName;
		
		public SampledHistogramCache(Path dataPath) {
			this.histFolder = Workbench.histFolder(dataPath);
			this.dataFileName = dataPath.getFileName().toString();
		}
		
		@SuppressWarnings("unchecked")
		public <T extends Number> SampledHistogram<T> getOrLoad(Attribute<T> a) {
			var hist = this._cache.get(a);
			if(hist == null) {
				var fpath = this.histFolder.resolve(Workbench.sampledHistName(dataFileName, a.name));
				try {
					hist = SampledHistogram.readFile(fpath);
				} catch (ClassNotFoundException | IOException e) {
					throw new RuntimeException(e);
				}
				this._cache.put(a, hist);
			}
			return (SampledHistogram<T>) hist;
		}
	}
	
	private final Map<Path, IntervalHistogramCache> _eqdCache = new HashMap<>();
	private final static IntervalHistogramCache.PerIntervalsCache.HistNameProvider eqdProvider = 
			new IntervalHistogramCache.PerIntervalsCache.HistNameProvider() {

				@Override
				public String name(String dataFileName, String attName, int intervals) {
					return Workbench.eqdHistName(dataFileName, attName, intervals);
				}

				@Override
				public <V extends Number> EquidistantHistogram<V> deserialize(Path path) {
					var hist = EquidistantHistogram.<V>readFile(path);
					return hist;
				}};
				
	private IntervalHistogramCache getEqdCache(Path path) {
		var c = this._eqdCache.get(path);
		if(c == null) {
			c = new IntervalHistogramCache(path, eqdProvider);
			this._eqdCache.put(path, c);
		}
		return c;
	}
				
	public <T extends Number> EquidistantHistogram<T> getOrLoadEqdHistogram(Path path, Attribute<T> a, int interval) {
		return (EquidistantHistogram<T>)this.getEqdCache(path).getOrLoad(a, interval);
	}
	
	public <T extends Number> Collection<EquidistantHistogram<T>> getOrLoadEqdHistograms(Path path, Attribute<T> a, Collection<Integer> is){
		var c = this.getEqdCache(path);
		return c.getOrLoadMany(a, is).stream().map(h -> (EquidistantHistogram<T>)h).toList();
	}
	
	public <T extends Number> Collection<EquidistantHistogram<T>> getOrLoadAllEqdHistograms(Path path, Attribute<T> a){
		var c = this.getEqdCache(path);
		return c.getOrLoadAll(a).stream().map(h -> (EquidistantHistogram<T>)h).toList();
	}
	
	private final Map<Path, IntervalHistogramCache> _eqnCache = new HashMap<>();
	private final static IntervalHistogramCache.PerIntervalsCache.HistNameProvider eqnProvider =
			new IntervalHistogramCache.PerIntervalsCache.HistNameProvider() {

				@Override
				public String name(String dataFileName, String attName, int intervals) {
					return Workbench.eqnHistName(dataFileName, attName, intervals);
				}

				@Override
				public <T extends Number> EquinominalHistogram<T> deserialize(Path path) {
					var hist = EquinominalHistogram.<T>readFile(path);
					return hist;
				}};
				
	private IntervalHistogramCache getEqnCache(Path path){
		var c = this._eqnCache.get(path);
		if(c == null) {
			c = new IntervalHistogramCache(
					path, eqnProvider);
			this._eqnCache.put(path, c);
		}
		return c;
	}
	
	public <V extends Number> EquinominalHistogram<V> getOrLoadEqnHistogram(Path path, Attribute<V> a, int i) {
		return (EquinominalHistogram<V>)this.getEqnCache(path).getOrLoad(a, i);
	}
	public <V extends Number> Collection<EquinominalHistogram<V>> getOrLoadAllEqnHistograms(Path path, Attribute<V> a){
		return this.getEqnCache(path).getOrLoadAll(a).stream().map(h -> (EquinominalHistogram<V>)h).toList();
	}
	public <V extends Number> Collection<EquinominalHistogram<V>> getOrLoadEqnHistograms(Path path, Attribute<V> a, Collection<Integer> is){
		return this.getEqnCache(path).getOrLoadMany(a, is).stream().map(h -> (EquinominalHistogram<V>)h).toList();
	}
	
	private static class IntervalHistogramCache {
		@SuppressWarnings("rawtypes")
		private final Map<Attribute, PerIntervalsCache> _cache = new HashMap<>();
		private final Path histFolder;
		private final String dataFileName;
		private final PerIntervalsCache.HistNameProvider provider;
		
		public IntervalHistogramCache(
				Path dataPath,
				PerIntervalsCache.HistNameProvider provider) {
			this.histFolder = Workbench.histFolder(dataPath);
			this.dataFileName = dataPath.getFileName().toString();
			this.provider = provider;
		}
		
		@SuppressWarnings("unchecked")
		private <V extends Number, U extends DataSlicedHistogram<V>> PerIntervalsCache<V, U> 
			getOrLoadCache(Attribute<V> a) {
			var c = this._cache.get(a);
			if(c == null) {
				c = new PerIntervalsCache<V, U>(
						this.histFolder,
						this.dataFileName,
						a,
						this.provider);
				this._cache.put(a, c);
			}
			return (PerIntervalsCache<V, U>)c;
		}
		
		public <V extends Number> DataSlicedHistogram<V> getOrLoad(Attribute<V> a, int i) {
			var c = this.getOrLoadCache(a);
			return c.getOrLoad(i);
		}
		
		public <V extends Number> Collection<DataSlicedHistogram<V>> getOrLoadMany(Attribute<V> a, Collection<Integer> intervals){
			var hists = intervals.stream()
					.map(i -> this.getOrLoad(a, i.intValue()))
					.collect(Collectors.toList());
			return hists;
		}
	
		public <V extends Number> Collection<DataSlicedHistogram<V>> getOrLoadAll(Attribute<V> a){
			var c = this.getOrLoadCache(a);
			return c.getOrLoadAll();
		}
		
		private static class PerIntervalsCache<V extends Number, U extends DataSlicedHistogram<V>> {
			private final Map<Integer, U> _cache = new HashMap<>();
			private final Path histFolder;
			private final String dataFileName;
			private final Attribute<V> attribute;
			private final HistNameProvider nameProvider;
			
			public static interface HistNameProvider {
				public String name(String dataFileName, String attName, int intervals);
				public <V extends Number> DataSlicedHistogram<V> deserialize(Path path);
			}
			
			public PerIntervalsCache(
					Path histFolder, 
					String dataFileName, 
					Attribute<V> attribute, 
					HistNameProvider nameProvider) {
				this.histFolder = histFolder;
				this.dataFileName = dataFileName;
				this.nameProvider = nameProvider;
				this.attribute = attribute;
			}
			
			@SuppressWarnings("unchecked")
			public U getOrLoad(int i) {
				var hist = this._cache.get(i);
				if(hist == null) {
					var fpath = this.histFolder.resolve(this.nameProvider.name(this.dataFileName, this.attribute.name, i));
					hist = (U) this.nameProvider.deserialize(fpath);
					this._cache.put(i, hist);
				}
				return hist;
			}
			
			public Collection<U> getOrLoadAll() {
				return this._cache.values().stream().collect(Collectors.toList());
			}
		}
	}
	
	private Map<Path, MCVCache> _mcvCache = new HashMap<>();
	public <T extends Number> MostCommonValues<T> getOrLoadMCV(Path table, Attribute<T> a) {
		var c = _mcvCache.get(table);
		if(c == null) {
			c = new MCVCache(table);
			this._mcvCache.put(table, c);
		}
		return c.getOrLoad(a);
	}
	
	private static class MCVCache {
		private final Map<Attribute<?>, MostCommonValues<?>> _cache = new HashMap<>();
		private final Path dataPath;
		
		public MCVCache(Path dataPath) {
			this.dataPath = dataPath;
		}
		
		@SuppressWarnings("unchecked")
		public <T extends Number> MostCommonValues<T> getOrLoad(Attribute<T> a) {
			var hist = (MostCommonValues<T>)this._cache.get(a);
			if(hist == null) {
				var fpath = Workbench.mcvFile(this.dataPath, a);
				try {
					hist = MostCommonValues.deserialize(Files.readString(fpath), a.domain);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				this._cache.put(a, hist);
			}
			return hist;
		}
	}
}
