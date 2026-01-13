package rq.common.restrictions;

import rq.common.onOperators.RecordValue;
import rq.common.similarities.ISimilarity;
import rq.common.table.Record;

public class Similar<T> extends BiCondition<T> {

	public final ISimilarity<T> similarity;
	
	public Similar(RecordValue<T> left, RecordValue<T> right, ISimilarity<T> similarity) {
		super(left, right);
		this.similarity = similarity;
	}
	
	@Override
	public double eval(Record record) {
		var lValue = this.left.value(record);
		var rValue = this.right.value(record);
		
		return this.similarity.apply(lValue, rValue);
	}

}
