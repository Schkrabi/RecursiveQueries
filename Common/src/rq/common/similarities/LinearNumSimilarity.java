package rq.common.similarities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LinearNumSimilarity implements ISimilarity<Double> {

	public final double similarUntil;
	
	@JsonCreator
	public LinearNumSimilarity(
			@JsonProperty("similarUntil")
			double similarUntil) {
		this.similarUntil = similarUntil;
	}
	
	@Override
	public Double apply(Double arg0, Double arg1) {
		var d = Math.abs(arg0 - arg1);
		var v = (-1.0d / this.similarUntil) * d + 1;
		return Math.max(0d, Math.min(1.0d, v));
	}
	
	@Override
	public int hashCode() {
		return new StringBuilder()
				.append(this.similarUntil)
				.toString().hashCode();
	}
	
	@Override
	public boolean equals(Object other) {
		if(other instanceof LinearNumSimilarity lns) {
			return this.similarUntil == lns.similarUntil;
		}
		return false;
	}

	@Override
	public String signature() {
		return new StringBuilder()
				.append("linDbl")
				.append(this.similarUntil)
				.toString();
	}
}
