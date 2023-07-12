/**
 * 
 */
package rq.common.operators;

import rq.common.exceptions.SchemaNotEqualException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.table.Schema;
import rq.common.table.Table;
import rq.common.table.TabularExpression;

/**
 * Represents union of two tabular expressions
 * @author Mgr. R.Skrabal
 *
 */
public class Union implements TabularExpression {
	private final TabularExpression argument1;
	private final TabularExpression argument2;
	
	private Union(TabularExpression argument1, TabularExpression argument2) {
		this.argument1 = argument1;
		this.argument2 = argument2;
	}
	
	/**
	 * Factory method
	 * @param argument1 first argument of union
	 * @param argument2 second argument of union
	 * @return Union instance
	 * @throws SchemaNotEqualException if argument schemas are not equal
	 */
	public static Union factory(TabularExpression argument1, TabularExpression argument2) 
		throws SchemaNotEqualException {
		Schema schema1 = argument1.schema();
		Schema schema2 = argument2.schema(); 
		if(!schema1.equals(schema2)) {
			throw new SchemaNotEqualException(schema1, schema2);
		}
		return new Union(argument1, argument2);
	}

	@Override
	public Table eval() {
		Table table = new Table(this.schema());
		this.argument1.eval().stream().forEach(r -> {
			try {
				table.insert(r);
			} catch (TableRecordSchemaMismatch e) {
				// Unlikely
				throw new RuntimeException(e);
			}
		});;
		this.argument2.eval().stream().forEach(r -> {
			try {
				table.insert(r);
			} catch (TableRecordSchemaMismatch e) {
				// Unlikely
				throw new RuntimeException(e);
			}
		});
		return table;
	}

	@Override
	public Schema schema() {
		return this.argument1.schema();
	}

}
