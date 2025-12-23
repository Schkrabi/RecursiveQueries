package rq.estimations.main;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.opencsv.exceptions.CsvValidationException;

import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.Table;
import rq.common.io.contexts.ClassNotInContextException;
import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.MostCommonValues;
import rq.common.statistic.SampledHistogram;
import rq.common.table.Attribute;
import rq.common.util.DeserializerRegistry;
import rq.files.exceptions.ColumnOrderingNotInitializedException;
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
	public SampledHistogram getOrLoadSampledHistogram(Path table, Attribute a) {
		var c = _sampledHistCache.get(a);
		if(c == null) {
			c = new SampledHistogramCache(table);
			this._sampledHistCache.put(table, c);
		}
		return c.getOrLoad(a);
	}
	
	private static class SampledHistogramCache {
		private final Map<Attribute, SampledHistogram> _cache = new HashMap<>();
		private final Path histFolder;
		private final String dataFileName;
		
		public SampledHistogramCache(Path dataPath) {
			this.histFolder = Workbench.histFolder(dataPath);
			this.dataFileName = dataPath.getFileName().toString();
		}
		
		public SampledHistogram getOrLoad(Attribute a) {
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
			return hist;
		}
	}
	
	private final Map<Path, IntervalHistogramCache<EquidistantHistogram>> _eqdCache = new HashMap<>();
	private final static IntervalHistogramCache.PerIntervalsCache.HistNameProvider<EquidistantHistogram> eqdProvider = 
			new IntervalHistogramCache.PerIntervalsCache.HistNameProvider<>() {

				@Override
				public String name(String dataFileName, String attName, int intervals) {
					return Workbench.eqdHistName(dataFileName, attName, intervals);
				}

				@Override
				public EquidistantHistogram deserialize(Path path) {
					var hist = EquidistantHistogram.readFile(path);
					return hist;
				}};
				
	private IntervalHistogramCache<EquidistantHistogram> getEqdCache(Path path) {
		var c = this._eqdCache.get(path);
		if(c == null) {
			c = new IntervalHistogramCache<EquidistantHistogram>(path, eqdProvider);
			this._eqdCache.put(path, c);
		}
		return c;
	}
				
	public EquidistantHistogram getOrLoadEqdHistogram(Path path, Attribute a, int interval) {
		return this.getEqdCache(path).getOrLoad(a, interval);
	}
	
	public Collection<EquidistantHistogram> getOrLoadEqdHistograms(Path path, Attribute a, Collection<Integer> is){
		var c = this.getEqdCache(path);
		return c.getOrLoadMany(a, is);
	}
	
	public Collection<EquidistantHistogram> getOrLoadAllEqdHistograms(Path path, Attribute a){
		var c = this.getEqdCache(path);
		return c.getOrLoadAll(a);
	}
	
	private final Map<Path, IntervalHistogramCache<EquinominalHistogram>> _eqnCache = new HashMap<>();
	private final static IntervalHistogramCache.PerIntervalsCache.HistNameProvider<EquinominalHistogram> eqnProvider =
			new IntervalHistogramCache.PerIntervalsCache.HistNameProvider<EquinominalHistogram>() {

				@Override
				public String name(String dataFileName, String attName, int intervals) {
					return Workbench.eqnHistName(dataFileName, attName, intervals);
				}

				@Override
				public EquinominalHistogram deserialize(Path path) {
					var hist = EquinominalHistogram.readFile(path);
					return hist;
				}};
	private IntervalHistogramCache<EquinominalHistogram> getEqnCache(Path path){
		var c = this._eqnCache.get(path);
		if(c == null) {
			c = new IntervalHistogramCache<EquinominalHistogram>(
					path, eqnProvider);
			this._eqnCache.put(path, c);
		}
		return c;
	}
	
	public EquinominalHistogram getOrLoadEqnHistogram(Path path, Attribute a, int i) {
		return this.getEqnCache(path).getOrLoad(a, i);
	}
	public Collection<EquinominalHistogram> getOrLoadAllEqnHistograms(Path path, Attribute a){
		return this.getEqnCache(path).getOrLoadAll(a);
	}
	public Collection<EquinominalHistogram> getOrLoadEqnHistograms(Path path, Attribute a, Collection<Integer> is){
		return this.getEqnCache(path).getOrLoadMany(a, is);
	}
	
	private static class IntervalHistogramCache<T extends DataSlicedHistogram> {
		private final Map<Attribute, PerIntervalsCache<T>> _cache = new HashMap<>();
		private final Path histFolder;
		private final String dataFileName;
		private final PerIntervalsCache.HistNameProvider<T> provider;
		
		public IntervalHistogramCache(
				Path dataPath,
				PerIntervalsCache.HistNameProvider<T> provider) {
			this.histFolder = Workbench.histFolder(dataPath);
			this.dataFileName = dataPath.getFileName().toString();
			this.provider = provider;
		}
		
		private PerIntervalsCache<T> getOrLoadCache(Attribute a) {
			var c = this._cache.get(a);
			if(c == null) {
				c = new PerIntervalsCache<T>(
						this.histFolder,
						this.dataFileName,
						a,
						this.provider);
				this._cache.put(a, c);
			}
			return c;
		}
		
		public T getOrLoad(Attribute a, int i) {
			var c = this.getOrLoadCache(a);
			return c.getOrLoad(i);
		}
		
		public Collection<T> getOrLoadMany(Attribute a, Collection<Integer> intervals){
			var hists = intervals.stream()
					.map(i -> this.getOrLoad(a, i.intValue()))
					.collect(Collectors.toList());
			return hists;
		}
		
		public Collection<T> getOrLoadAll(Attribute a){
			var c = this.getOrLoadCache(a);
			return c.getOrLoadAll();
		}
		
		private static class PerIntervalsCache<U extends DataSlicedHistogram> {
			private final Map<Integer, U> _cache = new HashMap<>();
			private final Path histFolder;
			private final String dataFileName;
			private final Attribute attribute;
			private final HistNameProvider<U> nameProvider;
			
			public static interface HistNameProvider<V extends DataSlicedHistogram> {
				public String name(String dataFileName, String attName, int intervals);
				public V deserialize(Path path);
			}
			
			public PerIntervalsCache(
					Path histFolder, 
					String dataFileName, 
					Attribute attribute, 
					HistNameProvider<U> nameProvider) {
				this.histFolder = histFolder;
				this.dataFileName = dataFileName;
				this.nameProvider = nameProvider;
				this.attribute = attribute;
			}
			
			public U getOrLoad(int i) {
				var hist = this._cache.get(i);
				if(hist == null) {
					var fpath = this.histFolder.resolve(this.nameProvider.name(this.dataFileName, this.attribute.name, i));
					hist = this.nameProvider.deserialize(fpath);
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
	public MostCommonValues getOrLoadMCV(Path table, Attribute a) {
		var c = _mcvCache.get(table);
		if(c == null) {
			c = new MCVCache(table);
			this._mcvCache.put(table, c);
		}
		return c.getOrLoad(a);
	}
	
	private static class MCVCache {
		private final Map<Attribute, MostCommonValues> _cache = new HashMap<>();
		private final Path dataPath;
		
		public MCVCache(Path dataPath) {
			this.dataPath = dataPath;
		}
		
		public MostCommonValues getOrLoad(Attribute a) {
			var hist = this._cache.get(a);
			if(hist == null) {
				var fpath = Workbench.mcvFile(this.dataPath, a);
				try {
					hist = MostCommonValues.deserialize(Files.readString(fpath));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				this._cache.put(a, hist);
			}
			return hist;
		}
	}
}
