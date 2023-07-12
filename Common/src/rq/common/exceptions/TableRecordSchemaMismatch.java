/**
 * 
 */
package rq.common.exceptions;

import rq.common.table.Schema;

/**
 * Raised if CRUD operation detects a mismatching schema
 * @author Mgr. R.Skrabal
 *
 */
public class TableRecordSchemaMismatch extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4898987819718202553L;
	public final Schema tableSchema;
	public final Schema recordSchema;
	
	public TableRecordSchemaMismatch(Schema tableSchema, Schema recordSchema) {
		super(new StringBuilder()
				.append("Record schema ")
				.append(recordSchema)
				.append(" mismatches table schema ")
				.append(tableSchema)
				.toString());
		this.tableSchema = tableSchema;
		this.recordSchema = recordSchema;
	}
}
