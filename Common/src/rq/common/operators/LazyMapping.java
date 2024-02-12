package rq.common.operators;

import java.util.function.Function;

import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.SchemaProvider;
import rq.common.statistic.Statistics;
import rq.common.table.Record;
import rq.common.table.Schema;

public class LazyMapping implements LazyExpression, SchemaProvider {

	private final Function<Record, Record> fun;
	private final LazyExpression argExp;
	private final Schema schema;
	
	private LazyMapping(LazyExpression argExp, Function<Record, Record> fun, Schema schema) {
		this.argExp = argExp;
		this.schema = schema;
		this.fun = fun;
	}
	
	public static <T extends LazyExpression & SchemaProvider> LazyMapping factory(T arg, Function<Record, Record> fun, Function<Schema, Schema> sFun) {
		SchemaProvider argSch = (SchemaProvider)arg;
		Schema schema = sFun.apply(argSch.schema());
		
		return new LazyMapping(arg, fun, schema);
	}
	
	public static <T extends LazyExpression & SchemaProvider> LazyMapping factory(T arg, Function<Record, Record> fun) {
		return LazyMapping.factory(arg, fun, Function.identity());
	}
	
	@Override
	public Schema schema() {
		return this.schema;
	}

	@Override
	public Record next() {
		Record record = this.argExp.next();
		if(record == null) {
			return null;
		}
		return this.fun.apply(record);
	}

	@Override
	public Statistics getStatistics() {
		return null;
	}

	@Override
	public boolean hasStatistics() {
		return false;
	}

}
