package rq.common.restrictions;

import rq.common.table.Record;
import rq.common.table.Schema;

public class Not implements SelectionCondition {
	
	private final SelectionCondition arg;

	public Not(SelectionCondition arg) {
		this.arg = arg;
	}

	@Override
	public double eval(Record record) {
		double value = this.arg.eval(record);
		return 1.0d - value;
	}

	@Override
	public boolean isApplicableToSchema(Schema schema) {
		return this.arg.isApplicableToSchema(schema);
	}

}
