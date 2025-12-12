package rq.common.estimations;

import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import rq.common.statistic.RankHistogram;

public class RandomEstimation implements IEstimation {
	
	private final Random rand;
	public final int size;
	public final int slices;

	public RandomEstimation(int slices, int size) {
		this.rand = new Random(System.currentTimeMillis());
		this.size = size;
		this.slices = slices;
	}
	
	public RandomEstimation(int slices, int size, Random rand) {
		this.rand = rand;
		this.size = size;
		this.slices = slices;
	}
	
	public RandomEstimation(int slices, int size, long seed) {
		this.rand = new Random(seed);
		this.size = size;
		this.slices = slices;
	}

	@Override
	public String signature() {
		return "rand";
	}

	@Override
	public RankHistogram estimate() {
		var vls = this.rand.doubles().limit(this.slices + 1).mapToObj(x -> x).collect(Collectors.toList());
		var sum = vls.stream().reduce((x, y) -> x + y).get();
		vls = vls.stream().map(v -> (v / sum) * this.size).collect(Collectors.toList());
		
		var slcs = RankHistogram.uniformSlices(this.slices);
		var i_vls = vls.iterator();
		var hist = new RankHistogram(slcs);
		for(var slc : slcs) {
			var cnt = i_vls.next();
			hist.addIntervalValue(slc, cnt);
		}
		//The last number is considered for tuples with rank 0
		
		return hist;
	}

	@Override
	public int getSlices() {
		return this.slices;
	}

	@Override
	public Map<String, String> _params() {
		return Map.of("slcs", Integer.toString(this.slices),
				"sz", Integer.toString(this.size));
	}

}
