package queries;

import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;

import rq.common.tools.Counter;

/**
 * Abstract class for defining queries of the experiments
 */
public abstract class Queries {
	
	/**
	 * Available algorithms
	 * @remark TopK & Transformed algorithm do not have equivalent expressive power. Unrestricted algorithm can express any query.
	 */
	public enum Algorithm {
		Unrestricted, TopK, Transformed
	}
	
	/**
	 * Algoithm to be run from this instance
	 */
	private final Algorithm algorithm;
	/**
	 * Counter used in the measurement
	 */
	protected final Counter recordCounter;
	
	protected Queries(Algorithm algorithm, Counter counter) {
		this.algorithm = algorithm;
		this.recordCounter = counter;
	}
	
	/**
	 * Returns identifier describing this experiment.
	 * This should contain name of the dataset, which algorithms are compared and parameters of the query
	 * @remark This identifier is used as file names
	 * @return identifier string
	 */
	protected abstract String algIdentificator();
	
	public String identificator() {
		return new StringBuilder()
				.append(this.algIdentificator())
				.append("_").append(this.algorithm)
				.toString();
	}
	
	/**
	 * Initializes prep query for the unrestricted algorithm
	 * @param iTable Input dataset loaded by stream from a file
	 * @return Lazy expression that prepares data for the unrestricted query
	 */
	protected abstract LazyExpression prepareUnrestricted(LazyExpression iTable);
	
	/**
	 * Initializes prep query for the topK algorithm
	 * @param iTable Input dataset loaded by stream from a file
	 * @return Lazy expression that prepares data for the topK query
	 */
	protected abstract LazyExpression prepareTopK(LazyExpression iTable);
	
	/**
	 * Initializes prep query for the transformed algorithm
	 * @param iTable Input dataset loaded by stream from a file
	 * @return Lazy expression that prepares data for the transformed query
	 */
	protected abstract LazyExpression prepareTransformed(LazyExpression iTable);
	
	/**
	 * Creates the data preparation query
	 * @param iTable input dataset loaded by stream from a file
	 * @return Lazy expression that prepares data for the query
	 */
	public LazyExpression prepare(LazyExpression iTable) {
		switch(this.algorithm) {
		case Unrestricted:
			return this.prepareUnrestricted(iTable);
		case TopK:
			return this.prepareTopK(iTable);
		case Transformed:
			return this.prepareTransformed(iTable);
		}
		throw new RuntimeException("Cannot recognize selected algorithm");
	}
	
	/**
	 * Initializes the query for the unrestricted algorithm
	 * @param iTable input table with prepared data
	 * @return query to be evaluated
	 */
	protected abstract TabularExpression queryUnterstricted(Table iTable);
	/**
	 * Initializes the query for the TopK algorithm
	 * @param iTable input table with prepared data
	 * @return query to be evaluated
	 */
	protected abstract TabularExpression queryTopK(Table iTable);
	/**
	 * Initializes the query for the Transformed algorithm
	 * @param iTable input table with prepared data
	 * @return query to be evaluated
	 */
	protected abstract TabularExpression queryTransformed(Table iTable);
	
	/**
	 * Initializes the query 
	 * @param iTable input table with prepared data
	 * @return query to be evaluated
	 */
	public TabularExpression query(Table iTable) {
		switch(this.algorithm) {
		case Unrestricted:
			return this.queryUnterstricted(iTable);
		case TopK:
			return this.queryTopK(iTable);
		case Transformed:
			return this.queryTransformed(iTable);
		}
		throw new RuntimeException("Cannot recognize selected algorithm.");
	}
	
	/**
	 * Postprocesses the data from the unrestricted algorithm query
	 * @param iTable output data of the query
	 * @return query to evaluate into postprocessed data
	 */
	protected abstract TabularExpression postprocessUnrestricted(Table iTable);
	
	/**
	 * Postprocesses the data from the topK algorithm query
	 * @param iTable output data of the query
	 * @return query to evaluate into postprocessed data
	 */
	protected abstract TabularExpression postprocessTopK(Table iTable);
	
	/**
	 * Postprocesses the data from the Transformed algorithm query
	 * @param iTable output data of the query
	 * @return query to evaluate into postprocessed data
	 */
	protected abstract TabularExpression postprocessTransformed(Table iTable);
	
	/**
	 * Postprocesses the data from the query
	 * @param iTable output data of the query
	 * @return query to evaluate into postprocessed data
	 */
	public TabularExpression postprocess(Table iTable) {
		switch(this.algorithm) {
		case Unrestricted:
			return this.postprocessUnrestricted(iTable);
		case TopK:
			return this.postprocessTopK(iTable);
		case Transformed:
			return this.postprocessTransformed(iTable);
		}
		throw new RuntimeException("Cannot recgnize selected algorithm");
	}
	
	protected RuntimeException algorithmNotSupportedException() {
		return new RuntimeException(new StringBuilder()
				.append("Algorithm ")
				.append(this.algorithm)
				.append(" is not supported for experiment ")
				.append(this.getClass().getAnnotation(CallingArg.class).value())
				.toString());
	}
	
}
