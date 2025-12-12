package rq.common.statistic;

import java.util.Random;

public interface IGeneratorProvider {

	/** Gets the random double generator based on this histogram*/
	public HistBasedRandom generator(Random rand);
	
	/** Gets the random double generator based on this histogram*/
	public default HistBasedRandom generator(long seed) {
		return this.generator(new Random(seed));
	}
	
	/** Gets the random double generator based on this histogram*/
	public default HistBasedRandom generator() {
		return this.generator(System.currentTimeMillis());
	}
}
