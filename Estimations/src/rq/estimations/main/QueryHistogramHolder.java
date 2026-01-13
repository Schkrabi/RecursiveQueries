package rq.estimations.main;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import rq.common.operators.LazySelection;
import rq.common.statistic.RankHistogram;
import rq.common.table.Attribute;
import rq.common.util.Pair;
import rq.estimations.framework.SelectionQueryInfo;

/** Gathers rank histograms from queries for all given slice numbers and stores them */
public class QueryHistogramHolder<T> {

	public final Collection<Integer> slices;
	private final Map<SelectionQueryInfo<T>, LazySelection> _selects;
	private Map<RankHistogramInfo<T>, RankHistogram> _hists = null;
	
	private QueryHistogramHolder(
			Collection<Integer> slices, 
			Map<SelectionQueryInfo<T>, LazySelection> selects) {
		this.slices = slices;
		this._selects = selects;
	}
	
	private Collection<Pair<RankHistogramInfo<T>, RankHistogram>> evalInfo(
			SelectionQueryInfo<T> info,
			LazySelection selection) {
		var start = System.currentTimeMillis();
		var record = selection.next();
		//Skip if empty
		if(record == null) {
			return List.of();
		}
		
		var hists = this.slices.stream()
				.map(s -> Pair.of(new RankHistogramInfo<>(info, s), new RankHistogram(s))) 
				.collect(Collectors.toList());
		
		while(record != null) {
			for(var hist : hists) {
				hist.second.addRank(record.rank);
			}
			record = selection.next();
		}
		var end = System.currentTimeMillis();
		
		System.out.println(info.queryFileNameBase() + " duration " + Duration.ofMillis(end - start).toString());
		return hists;
	}
	
	public Map<RankHistogramInfo<T>, RankHistogram> getHists(){
		if(this._hists == null) {
			this._hists = new HashMap<>();
			for(var e : this._selects.entrySet()) {
				var hs = this.evalInfo(e.getKey(), e.getValue());
				for(var p : hs) {
					this._hists.put(p.first, p.second);
					try {
						Files.writeString(Workbench.histFolder(p.first.queryInfo.dataPath).resolve(p.first.fileName()),
								p.second.serialize());
					} catch (IOException e1) {
						throw new RuntimeException(e1);
					}
				}
			}
		}
		return this._hists;
	}
	
	public Collection<Pair<RankHistogramInfo<T>, RankHistogram>> getHistograms(Attribute<T> a, int slice){
		var hists = this.getHists();
		var fltrd = hists.entrySet().stream()
				.filter(e -> e.getKey().slice == slice && e.getKey().queryInfo.attribute.equals(a))
				.map(e -> Pair.of(e.getKey(), e.getValue()))
				.collect(Collectors.toList());
		return fltrd;
	}
	
	public static <T> QueryHistogramHolder<T> fromRestrictionQueries(
			Collection<Integer> slices,
			RestrictionQueries<T> rq) {
		var selects = rq.getSelections();
		var me = new QueryHistogramHolder<T>(slices, selects);
		return me;
	}

	public static class RankHistogramInfo<T> {
		public final SelectionQueryInfo<T> queryInfo;
		public final int slice;
		
		public RankHistogramInfo(
				SelectionQueryInfo<T> queryInfo,
				int slice) {
			this.queryInfo = queryInfo;
			this.slice = slice;
		}
		
		public String fileNameBase() {
			return new StringBuilder()
					.append(this.queryInfo.queryFileNameBase())
					.append(".slc=")
					.append(this.slice)
					.toString();
		}
		
		public String fileName() {
			return new StringBuilder()
					.append(this.fileNameBase())
					.append(".hist")
					.toString();
		}
		
		@Override
		public String toString() {
			return this.fileNameBase();
		}
		
		@Override
		public int hashCode() {
			return new StringBuilder()
					.append(this.queryInfo.hashCode())
					.append(Integer.hashCode(this.slice))
					.toString().hashCode();
		}
		
		@Override
		public boolean equals(Object other) {
			if(other instanceof RankHistogramInfo) {
				var rhi = (RankHistogramInfo<?>)other;
				return this.queryInfo.equals(rhi.queryInfo)
						&& this.slice == rhi.slice;
			}
			return false;
		}
	}
}
