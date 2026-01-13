package rq.estimations.framework;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import rq.common.estimations.IEstimation;
import rq.common.estimations.ReintroduceRanks;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SlicedStatistic.RankInterval;
import rq.common.table.Attribute;
import rq.common.util.Pair;
import rq.estimations.contracts.RestrictionExperimentContract;
import rq.estimations.main.Measurement;
import rq.estimations.main.QueryHistogramHolder;
import rq.estimations.main.RestrictionQueries;
import rq.estimations.main.Workbench;
import rq.estimations.main.QueryHistogramHolder.RankHistogramInfo;

public class RestrictionExperiment {

	protected RestrictionExperimentContract contract;
	
	public RestrictionExperiment(
			RestrictionExperimentContract cnt) {
		this.contract = cnt;
		
//		this.dataPath = dataPath;
//		this.dataFileName = this.dataPath.getFileName().toString();
//		this.similarUntil = similarUntil;
//		this.queryCount = queryCount;
//		this.attributes = attributes;
//		this.slices = slices;
//		this.intervals = intervals;
//		this.consideredValues = consideredValues;
//		this.random = random;
//		this.paretRatios = paretRatios;
//		
//		for(var e : similarUntil.entrySet()) {
//			this.similarity.put(e.getKey(), LinearSimilarities.doubleSimilarityUntil(e.getValue()));
//		}
//		this.histFolder = Workbench.histFolder(dataPath);
//		this.estFolder = Workbench.estFolder(dataPath);
//		this.USE_RANKED_TABLE_AS_PRIMARY_DATA = useRankedTableAsPrimaryData;
//		this.queryGenerationStrategy = queryGenerationStrategy;
//		this.queryValues = queryValues;
	}
	
	private Map<Attribute<Double>, Map<Integer, NumericalEstimations>> _numericalEsts = 
			new HashMap<>();
	
	private Map<Integer, NumericalEstimations> _getPerIntervalEsts(Attribute<Double> a){
		var m = this._numericalEsts.get(a);
		if(m == null) {
			m = new HashMap<>();
			this._numericalEsts.put(a, m);
		}
		return m;
	}
	
	public NumericalEstimations numericalEst(Attribute<Double> a, int slice) {
		var perInterval = this._getPerIntervalEsts(a);
		var ne = perInterval.get(slice);
		if(ne == null) {
			ne = new NumericalEstimations(
					this.contract.getDataPath(),
					a,
					slice,
					this.contract.getSimilarity().get(a),
					this.contract.getIntervals().get(a),
					this.contract.getConsideredValues().get(a),
					this.contract.getParetRatios().get(a));
			perInterval.put(slice, ne);
		}
		return ne;
	}
	
	private final Map<Attribute<Double>, Map<Integer, Collection<Pair<IEstimation, RankHistogram>>>> _estmatedHists = new HashMap<>();
	private Map<Integer, Collection<Pair<IEstimation, RankHistogram>>> _getPerIntervalEstHists(Attribute<Double> a){
		var m = this._estmatedHists.get(a);
		if(m == null) {
			m = new HashMap<>();
			this._estmatedHists.put(a, m);
		}
		return m;
	}
	
	private Collection<Pair<IEstimation, RankHistogram>> estimates(Attribute<Double> a, int slice){
		var m = this._getPerIntervalEstHists(a);
		var ests = m.get(slice);
		if(ests == null) {
			ests = new ArrayList<>();
			var ne = this.numericalEst(a, slice);

			for (var est : ne.getEstmations()) {
				RankHistogram finalRanks;
				if(this.contract.isUseRankedDataAsPrimary())
				{
					var orgRanks = RankHistogram.readFile(this.contract.getHistFolder()
							.resolve(Workbench.rankHistFileName(this.contract.getDataFileName(), est.getSlices())));
					var ranks = est.estimate();
					finalRanks = ReintroduceRanks.recalculate(ranks, orgRanks);
				}
				else
				{
					finalRanks = est.estimate();
				}
				ests.add(Pair.of(est, finalRanks));
				m.put(slice, ests);
				try {
					Files.writeString(Workbench.restrictionEstimationPath(this.contract.getEstFolder(), a, est),
							finalRanks.serialize(), StandardOpenOption.CREATE, StandardOpenOption.WRITE,
							StandardOpenOption.TRUNCATE_EXISTING);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return ests;
	}
	
	private Map<Attribute<Double>, RestrictionQueries<Double>> _restrictionQueries = new HashMap<>();
	
	private RestrictionQueries<Double> getRestrictionQueries(Attribute<Double> a) {
		var rq = this._restrictionQueries.get(a);
		if(rq == null) {
			switch(this.contract.getQueryGenerationStrategy()){
			case SampledBasedParet:
				var hist = ResourceLoader.instance().getOrLoadSampledHistogram(this.contract.getDataPath(), a);
				rq = new RestrictionQueries.Paret(this.contract.getDataPath(), a, this.contract.getQueryCount(), this.contract.getSimilarity().get(a), hist);
				break;
			case IntervalBasedParet:
				var eqn = ResourceLoader.instance().getOrLoadEqnHistogram(this.contract.getDataPath(), a, 
						this.contract.getIntervals().get(a).stream().reduce(Math::max).get());
				rq = new RestrictionQueries.Paret(this.contract.getDataPath(), a, this.contract.getQueryCount(), this.contract.getSimilarity().get(a), eqn);
				break;
			case Uniform:
				rq = new RestrictionQueries.Uniform(this.contract.getDataPath(), a, this.contract.getQueryCount(), this.contract.getSimilarity().get(a), this.contract.getRandom());
				break;
			case Values:
				rq = new RestrictionQueries.FromValues<>(this.contract.getDataPath(), a, this.contract.getSimilarity().get(a), this.contract.getQueryValues().get(a));
			}
			this._restrictionQueries.put(a, rq);
		}
		return rq;
	}
	
	private Map<Attribute<Double>, QueryHistogramHolder<Double>> _queryHists = new HashMap<>();
	private QueryHistogramHolder<Double> getQryHistHolder(Attribute<Double> a) {
		var qhh = this._queryHists.get(a);
		if(qhh == null) {
			qhh = QueryHistogramHolder.fromRestrictionQueries(this.contract.getSlices(), this.getRestrictionQueries(a));
			this._queryHists.put(a, qhh);
		}		
		return qhh;
	}
	
	private Collection<Pair<RankHistogramInfo<Double>, RankHistogram>> qryHists(Attribute<Double> a, int slice){
		return this.getQryHistHolder(a).getHistograms(a, slice);
	}
	
	private Collection<IEstimation> allEstimations(){
		var l = new ArrayList<IEstimation>();
		for(var a : this.contract.getAttributes()) {
			for(var slice : this.contract.getSlices()) {
				this.estimates(a, slice).stream().forEach(p -> l.add(p.first));
				
				var qrhst = this.qryHists(a, slice);
				for(var query : qrhst) {
					this.constantExperiment(query.first).estimations().stream().forEach(e -> l.add(e));
				}
			}
		}
		return l;
	}
	
	private Collection<String> _columns = null;
	protected Collection<String> columns(){
		if(_columns == null) {
			this._columns = new LinkedHashSet<String>();
			for(var e : this.allEstimations()) {
				this._columns.addAll(e.params().keySet());
			}
			
		}
		return this._columns;
	}
	
	private Collection<RankInterval> _measuredIntervals = null;
	protected Collection<RankInterval> measuredIntervals(){
		if(this._measuredIntervals == null) {
			this._measuredIntervals = new ArrayList<RankInterval>();
			for(var s : this.contract.getSlices()) {
				var itvs = RankHistogram.uniformSlices(s);
				this._measuredIntervals.addAll(itvs);
			}
		}
		return this._measuredIntervals;
	}
	
	private String _header = null;
	protected String header() {
		if(this._header == null) {
			this._header = new StringBuilder()
					.append(this.columns().stream().reduce((f, s) -> f + "," + s).get())
					.append(",")
					.append(this.measuredIntervals().stream()
								.map(i -> i.toString().replace(',', ';'))
								.reduce((f, s) -> f +  "," + s).get())
					.append(",")
					.append(this.measuredIntervals().stream()
							.map(i -> "Ratio:" + i.toString().replace(',', ';'))
							.reduce((f, s) -> f +  "," + s).get())
					.append(",query,accuracy,inaccuracy")
					.toString();
		}
		return this._header;
	}
	
	private Map<RankHistogramInfo<Double>, ConstantRestrictionExperiment> _cnstEsts = new HashMap<>();
	public ConstantRestrictionExperiment constantExperiment(RankHistogramInfo<Double> info) {
		var exp = this._cnstEsts.get(info);
		if(exp == null) {
			exp = new ConstantRestrictionExperiment(info, this.contract.getConsideredValues().get(info.queryInfo.attribute));
			_cnstEsts.put(info, exp);
		}
		return exp;
	}
	
	protected String line(IEstimation est, RankHistogram ehist, RankHistogramInfo<Double> query, RankHistogram qhist, int size) {
		var params = est.params();
		var sb = new StringBuilder();
		for(var p : this.columns()) {
			String v = null;
			if(p.equals("att")) {
				v = params.get(p);
				if(v == null) {
					v = query.queryInfo.attribute.name;
				}
			}
			else {
				v = params.getOrDefault(p, "");
			}
			sb.append(v);
			sb.append(",");
		}
		
		//Absolute differences
		for(var i : this.measuredIntervals()) {
			if(ehist.contains(i)) {
				//Negative value: estimation overstimates
				//Positive value: estimation underestimates
				var esl = ehist.get(i);
				var qsl = qhist.get(i);
				sb.append(Double.toString(esl - qsl))
					.append(",");
			}
			else {
				sb.append(",");
			}
		}
		
		//Ratios
		for(var i : this.measuredIntervals()) {
			if(ehist.contains(i)) {
				//Negative value: estimation underestimates
				//Positive value: estimation overestimates
				var esl = ehist.get(i);
				var qsl = qhist.get(i);
				sb.append(Double.toString((esl - qsl) / size))
					.append(",");
			}
			else {
				sb.append(",");
			}
		}
		
		sb.append(query.fileName())
			.append(",")
			.append(Measurement.accuracy(ehist, qhist, size))
			.append(",")
			.append(Measurement.inaccuracy(ehist, qhist, size));
		return sb.toString();
	}

	public void gatherData() {
		var sb = new StringBuilder(this.header()).append("\n");
		var size = ResourceLoader.instance().getOrLoadTable(this.contract.getDataPath()).size();
		
		for(var a : this.contract.getAttributes()) {
			for(var slice : this.contract.getSlices()) {
				var qrhst = this.qryHists(a, slice);
				var ests = this.estimates(a, slice);
				for(var query : qrhst) {
					//General estimations
					for(var est : ests) {
						sb.append(this.line(
								est.first, 
								est.second, 
								query.first, 
								query.second, 
								size))
							.append('\n');
					}
					
					//Constant estimations
					for(var est : this.constantExperiment(query.first).estimations()) {
						var hest = est.estimate();
						sb.append(this.line(
								est, 
								hest, 
								query.first, 
								query.second, 
								size))
							.append("\n");
						
						try {
							hest.writeFile(Workbench.estFolder(this.contract.getDataPath()).resolve(est.filename()));
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}
				}
			}
		}
		try {
			Files.writeString(Workbench.restrictionResultFile(this.contract.getDataPath()), sb.toString());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
