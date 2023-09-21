/**
 * 
 */
package rq.common.table;

import java.util.ArrayList;
import java.util.Iterator;

import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.LazyIterable;
import rq.common.interfaces.LazyIterator;
import rq.common.interfaces.SchemaProvider;

/**
 * @author Mgr. Radomir Skrabal
 *
 */
public class CachedExpression implements LazyIterable, SchemaProvider, LazyExpression  {

	protected ArrayList<rq.common.table.Record> cache = new ArrayList<rq.common.table.Record>();
	private final LazyExpression argExp;
	private final SchemaProvider argSch;
	
	private CachedExpression(LazyExpression argExp, SchemaProvider argSch) {
		this.argExp = argExp;
		this.argSch = argSch;
	}
	
	public static <T extends LazyExpression & SchemaProvider> CachedExpression factory(T argument) {
		return new CachedExpression((LazyExpression)argument, (SchemaProvider)argument);
	}
	
	public static class CachedExpressionIterator implements LazyIterator{
		
		private Iterator<rq.common.table.Record> innerIterator;
		private final CachedExpression tabular;
		private Record current = null;
		
		private CachedExpressionIterator(CachedExpression tabular, Iterator<rq.common.table.Record> innerIterator) {
			this.innerIterator = innerIterator;
			this.tabular = tabular;
		}

		@Override
		public Record next() {
			if(innerIterator != null
				&&	innerIterator.hasNext()) {
				this.current = innerIterator.next();
				return this.current;
			}
			this.innerIterator = null;
			this.current = this.tabular.next(); 
			return this.current;
		}

		@Override
		public Record current() {
			return this.current;
		}

		@Override
		public void restart() {
			this.innerIterator = this.tabular.cache.iterator();
			this.current = null;			
		}
		
	}
	
	@Override
	public Record next() {
		Record record = this.argExp.next();
		this.cache.add(record);
		return record;
	}
	

	@Override
	public LazyIterator lazyIterator() {
		return new CachedExpressionIterator(this, this.cache.iterator());
	}
	
	@Override
	public Schema schema() {
		return this.argSch.schema();
	}
}
