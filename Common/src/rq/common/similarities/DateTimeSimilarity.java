package rq.common.similarities;

import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import rq.common.types.DateTime;

public class DateTimeSimilarity implements ISimilarity<DateTime> {

	public final long similarUntil;
	
	@JsonCreator
	public DateTimeSimilarity(
			@JsonProperty("similarUntil")
			long similarUntil) {
		this.similarUntil = similarUntil;
	}
	
	@Override
	public Double apply(DateTime t1, DateTime t2) {
		var ldt1 = ((DateTime)t1).getInner();
		var ldt2 = ((DateTime)t2).getInner();
		var d = Duration.between(ldt1, ldt2).toSeconds();
		var v = (-1.0d / this.similarUntil) * d + 1;
		return Math.max(0d, Math.min(1.0d, v));
	}

	@Override
	public boolean equals(Object other) {
		if(other instanceof DateTimeSimilarity ds) {
			return this.similarUntil == ds.similarUntil;
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return Long.hashCode(this.similarUntil);
	}

	@Override
	public String signature() {
		return new StringBuilder()
				.append("dtu")
				.append(this.similarUntil)
				.toString();
	}
}
