package rq.common.onOperators;

import rq.common.table.Record;
import rq.common.table.Schema;

public class Constant<T> implements RecordValue<T> {

	T value;
	
	public Constant(T value) {
		this.value = value;
	}
	
	@Override
	public T value(Record record) {
		return this.value;
	}

	@Override
	public boolean isApplicableToSchema(Schema schema) {
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<T> domain() {
		return (Class<T>)this.value.getClass();
	}

	@Override
	public String toString() {
		return this.value.toString();
	}
}
