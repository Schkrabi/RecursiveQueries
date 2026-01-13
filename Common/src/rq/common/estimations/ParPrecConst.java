package rq.common.estimations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import rq.common.statistic.RankHistogram;
import rq.common.table.Attribute;
import rq.common.util.Pair;
import rq.common.similarities.ISimilarity;

/** Estimation based on (precise) most common values */
public class ParPrecConst implements IEstimation {

	public final Attribute<Double> attriute;
	public final int slices;
	public final double c;
	public final ISimilarity<Double> similarity;
	private final Collection<Pair<Double, Integer>> mostCommon;
	
	public ParPrecConst(
			Attribute<Double> attribute,
			int slices,
			double c,
			ISimilarity<Double> similarity,
			Collection<Pair<Double, Integer>> mostCommon) {
		this.attriute = attribute;
		this.slices = slices;
		this.c = c;
		this.similarity = similarity;
		this.mostCommon = new ArrayList<>(mostCommon);
	}

	@Override
	public String signature() {
		return "Ppc";
	}

	@Override
	public RankHistogram estimate() {
		var est = new RankHistogram(this.slices);
		for(var p : this.mostCommon) {
			var current = new RankHistogram(this.slices);
			var rank = this.similarity.apply(this.c, p.first);
			current.setIntervalValue(current.fit(rank), p.second);
			
			est = RankHistogram.add(est, current);
		}
		return est;
	}

	@Override
	public int getSlices() {
		return this.slices;
	}

	@Override
	public Map<String, String> _params() {
		return Map.of("slcs", Integer.toString(this.slices),
				"vls", Integer.toString(this.mostCommon.size()),
				"c", Double.toString(this.c),
				"att", this.attriute.name);
	}

}
