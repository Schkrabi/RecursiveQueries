package rq.common.operators;

import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.latices.LaticeFactory;
import rq.common.restrictions.SelectionCondition;
import rq.common.statistic.Statistics;
import rq.common.table.MemoryTable;
import rq.common.table.Schema;
import rq.common.table.Record;

import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import rq.common.exceptions.SelectionConditionNotApplicableToSchema;
import rq.common.exceptions.TableRecordSchemaMismatch;

/**
 * Class representing not-pipelined restriction with structured condition
 */
public class Selection extends AbstractSelection implements TabularExpression {
	
	public final TabularExpression arg;
	private final BiFunction<Schema, Integer, Table> tableSupplier;
	public Selection(
			TabularExpression arg, 
			SelectionCondition condition,
			BiFunction<Schema, Integer, Table> tableSupplier,
			BinaryOperator<Double> product) {
		super(condition, product);
		if(!condition.isApplicableToSchema(arg.schema())) {
			throw new RuntimeException(new SelectionConditionNotApplicableToSchema(condition, arg.schema()));
		}
		
		this.arg = arg;
		this.tableSupplier = tableSupplier;
	}
	
	public Selection(TabularExpression arg, SelectionCondition condition) {
		super(condition, LaticeFactory.instance().getProduct());
		if(!condition.isApplicableToSchema(arg.schema())) {
			throw new RuntimeException(new SelectionConditionNotApplicableToSchema(condition, arg.schema()));
		}
		
		this.arg = arg;
		this.tableSupplier = (Schema s, Integer count) -> new MemoryTable(s);
	}
	
	@Override
	public Table eval() {
		Table t = this.arg.eval();
		Table ret = this.tableSupplier.apply(this.schema(), t.size());
		
		for(Record r : t) {
			double rank = this.recordSatisfaction(r);
			if(rank > 0.0d) {
				try {
					ret.insert(new Record(r, rank));
				} catch (TableRecordSchemaMismatch e) {
					// Unlikely
					throw new RuntimeException(e);
				}
			}
		}
		return ret;
	}

	@Override
	public Schema schema() {
		return this.arg.schema();
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
