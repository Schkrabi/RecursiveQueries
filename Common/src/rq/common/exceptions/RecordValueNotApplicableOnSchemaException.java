package rq.common.exceptions;

import rq.common.onOperators.RecordValue;
import rq.common.table.Schema;

public class RecordValueNotApplicableOnSchemaException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4172456359582852084L;
	public final RecordValue recordValue;
	public final Schema schema;
	
	public RecordValueNotApplicableOnSchemaException(RecordValue recordValue, Schema schema) {
		super(new StringBuilder()
				.append("Record value ")
				.append(recordValue)
				.append(" is not applicable to schema ")
				.append(schema)
				.toString());
		this.recordValue = recordValue;
		this.schema = schema;
	}
}
