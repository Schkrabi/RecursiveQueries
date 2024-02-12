package rq.common.restrictions;

import java.util.function.BiFunction;

import rq.common.onOperators.RecordValue;
import rq.common.table.Record;

public class Similar extends BiCondition {

	public final BiFunction<Object, Object, Double> similarity;
	
	public Similar(RecordValue left, RecordValue right, BiFunction<Object, Object, Double> similarity) {
		super(left, right);
		this.similarity = similarity;
	}
	
	@Override
	public double eval(Record record) {
		Object lValue = this.left.value(record);
		Object rValue = this.right.value(record);
		
		return this.similarity.apply(lValue, rValue);
	}

}
