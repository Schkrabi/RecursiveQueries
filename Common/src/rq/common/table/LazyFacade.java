package rq.common.table;

import java.util.Iterator;

import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.SchemaProvider;
import rq.common.interfaces.Table;

/**
 * Lazy facade for tables
 * 
 * @author Mgr. Radomir Skrabal
 *
 */
public class LazyFacade implements LazyExpression, SchemaProvider {
	
	private final Table table;
	private final Iterator<Record> it;
	
	public LazyFacade(Table table) {
		this.table = table;
		this.it = table.iterator();
	}

	@Override
	public Schema schema() {
		return this.table.schema();
	}

	@Override
	public Record next() {
		if(it.hasNext()) {
			return it.next();
		}
		return null;
	}

}
