package rq.common.estimations;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import rq.common.statistic.RankHistogram;

public interface IEstimation extends SignatureProvider, IParametrized{

	/** Calculates the estimate */
	public RankHistogram estimate();
	
	/** Returns number of slices*/
	public int getSlices();
	
	public abstract Map<String, String> _params();
	
	public default Map<String, String> params(){
		var m = new HashMap<>(Map.of("signature", this.signature()));
		m.putAll(this._params());
		return m;		
	}
	
	/** Filename of the estimation file*/
	public default String filename() {
		return new StringBuilder()
				.append(this.signature())
				.append(".")
				.append(this.paramStr())
				.append(".est")
				.toString();
	}
}
