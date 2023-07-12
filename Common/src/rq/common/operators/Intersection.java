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
 * @author r.skrabal
 *
 */
public class Intersection implements TabularExpression {
	private final TabularExpression argument1, argument2;
	
	private Intersection(TabularExpression argument1, TabularExpression argument2) {
		this.argument1 = argument1;
		this.argument2 = argument2;
	}
	
	/**
	 * Factory method
	 * @param argument1
	 * @param argument2
	 * @return Intersection instance
	 * @throws SchemaNotEqualException
	 */
	public static Intersection factory(TabularExpression argument1, TabularExpression argument2)
		throws SchemaNotEqualException {
		Schema schema1 = argument1.schema();
		Schema schema2 = argument2.schema(); 
		if(!schema1.equals(schema2)) {
			throw new SchemaNotEqualException(schema1, schema2);
		}
		return new Intersection(argument1, argument2);
	}

	@Override
	public Table eval() {
		Table table = new Table(this.schema());
		Table table2 = this.argument2.eval();
		this.argument1.eval().stream()
			.filter(r -> table2.contains(r))
			.forEach(r -> {
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
