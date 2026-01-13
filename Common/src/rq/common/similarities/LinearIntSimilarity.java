package rq.common.similarities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LinearIntSimilarity implements ISimilarity<Integer> {

	public final int similarUntil;
	
	@JsonCreator
	public LinearIntSimilarity(
			@JsonProperty("similarUntil") int until) {
		this.similarUntil = until;
	}
	
	@Override
	public Double apply(Integer a0, Integer a1) {
		var arg0 = a0.doubleValue();
		var arg1 = a1.doubleValue();
		
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
		if(other instanceof LinearIntSimilarity lns) {
			return this.similarUntil == lns.similarUntil;
		}
		return false;
	}

	@Override
	public String signature() {
		return new StringBuilder()
				.append("linInt")
				.append(this.similarUntil)
				.toString();
	}

}
