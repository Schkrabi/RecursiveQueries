package rq.common.exceptions;

import rq.common.onOperators.OnOperator;
import rq.common.table.Schema;

public class OnOperatornNotApplicableToSchemaException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3922051512969216226L;
	public final OnOperator onOperator;
	public final Schema leftSchema;
	public final Schema rightSchema;
	
	public OnOperatornNotApplicableToSchemaException(OnOperator onOperator, Schema leftSchema, Schema rightSchema) {
		super(new StringBuilder()
				.append("OnOperator ")
				.append(onOperator.toString())
				.append(" is not applicable on schemas ")
				.append(leftSchema.toString())
				.append(" and ")
				.append(rightSchema.toString())
				.toString());
		this.onOperator = onOperator;
		this.leftSchema = leftSchema;
		this.rightSchema = rightSchema;
	}

}
