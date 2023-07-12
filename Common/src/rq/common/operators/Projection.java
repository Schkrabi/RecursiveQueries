/**
 * 
 */
package rq.common.operators;

import java.util.stream.Collectors;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.NotSubschemaException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.table.Schema;
import rq.common.table.Table;
import rq.common.table.TabularExpression;
import rq.common.table.Record;

/**
 * Represents a projection operator
 * @author Mgr. R.Skrabal
 *
 */
public class Projection implements TabularExpression {
	
	private final Schema schema;
	private final TabularExpression argument;
	
	private Projection(TabularExpression argument, Schema schema) {
		this.schema = schema;
		this.argument = argument;
	}
	
	/**
	 * Factory method
	 * @param argument
	 * @param schema
	 * @return
	 * @throws NotSubschemaException
	 */
	public static Projection factory(TabularExpression argument, Schema schema) 
		throws NotSubschemaException{
		if(!argument.schema().isSubSchema(schema)) {
			throw new NotSubschemaException(argument.schema(), schema);
		}
		
		return new Projection(argument, schema);
	}
	
	/**
	 * Projects a single record
	 * @param record
	 * @return
	 * @throws TypeSchemaMismatchException
	 */
	private Record project(Record record) throws TypeSchemaMismatchException {
		try {
			return 
				Record.factory(
						this.schema,
						this.schema.stream()
							.map(a -> {
								try {
									return new Record.AttributeValuePair(a, record.get(a));
								} catch (AttributeNotInSchemaException e) {
									// Unlikely
									throw new RuntimeException(e);
								}
							}).collect(Collectors.toList()),
						record.rank);
		} catch (TypeSchemaMismatchException | AttributeNotInSchemaException e) {
			// Unlikely
			throw new RuntimeException(e);
		}
	}

	@Override
	public Table eval() {
		Table source = this.argument.eval();
		Table dest = new Table(this.schema);
		
		for(Record r : source) {
			try {
				dest.insert(this.project(r));
			} catch (TableRecordSchemaMismatch | TypeSchemaMismatchException e) {
				// Unlikely
				return null;
			}
		}
		
		return dest;
	}

	@Override
	public Schema schema() {
		return this.schema;
	}

}
