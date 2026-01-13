package rq.common.estimations;

import rq.common.interfaces.Table;
import rq.common.onOperators.Constant;
import rq.common.operators.Selection;
import rq.common.statistic.RankHistogram;
import rq.common.table.Attribute;
import rq.common.restrictions.Similar;

public abstract class AbstractSelectionEstimation<T> {
	
	public final Selection selection;
	public final Table argument;
	public final int resultSlices;
	public final Attribute<T> attribute;
	public final Constant<Object> constant;
	
	public final rq.common.similarities.ISimilarity<T> similarity;
	protected final Similar<T> condition;

	@SuppressWarnings("unchecked")
	public AbstractSelectionEstimation(
			Selection selection, 
			int resultSlices) {
		this.argument = (Table) selection.arg;
		
		if(!this.argument.hasStatistics()) {
			throw new RuntimeException("Argument must provide statistics");
		}
		this.selection = selection;
		this.resultSlices = resultSlices;
		
		if(!(selection.condition instanceof Similar)) {
			throw new RuntimeException("Selection must have a single bicondition.");
		}
		this.condition = (Similar<T>)selection.condition;
		this.similarity = this.condition.similarity;
		
		if(!(condition.left instanceof Attribute)) {
			throw new RuntimeException("Left argument of condition must be an attribute");
		}
		this.attribute = (Attribute<T>)condition.left;
		
		if(!(condition.right instanceof Constant)) {
			throw new RuntimeException("Right argument of condition must be a constant.");
		}
		this.constant = (Constant<Object>)this.condition.right;
	}
	
	/**
	 * Estimates without probes
	 * @return estimate 
	 */
	public abstract RankHistogram estimate();
	
	
	
	
	
	
	
	
	
	

	
}
