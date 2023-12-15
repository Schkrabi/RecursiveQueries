package queries;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import algorithmBuilder.AlgorithmBuilder;
import annotations.CallingArg;
import annotations.QueryParameter;
import annotations.QueryParameterGetter;
import rq.common.algorithms.LazyRecursive;
import rq.common.annotations.Algorithm;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.tools.AlgorithmMonitor;

public abstract class Queries2 {
	private final Class<? extends LazyRecursive> algorithm;
	private AlgorithmBuilder algorithmBuilder = null;
	
	/**
	 * Counter used in the measurement
	 */
	protected final AlgorithmMonitor monitor;
	
	protected int k = 1;
	
	@QueryParameter("K")
	public void setK(String k) {
		this.k = Integer.parseInt(k);
	}
	
	@QueryParameterGetter("K")
	public String getK() {
		return Integer.toString(this.k);
	}
	
	protected Queries2(
			Class<? extends LazyRecursive> algorithm,
			AlgorithmMonitor monitor) {
		this.algorithm = algorithm;
		this.monitor = monitor;
	}
	
	private AlgorithmBuilder getBuilder() {
		if(this.algorithmBuilder == null) {
			try {
			this.algorithmBuilder = AlgorithmBuilder.construct(
					this.algorithm, 
					this.initialProvider(), 
					this.recursiveStepProvider(), 
					this.postprocessProvider(),
					this.k);
			}catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
		return this.algorithmBuilder;
	}
	
	/**
	 * Returns identifier describing this experiment.
	 * This should contain name of the dataset, which algorithms are compared and parameters of the query
	 * @remark This identifier is used as file names
	 * @return identifier string
	 */
	protected abstract String algIdentificator();
	
	/**
	 * Returns the provider of recusrive algorithm initial step query
	 * @return
	 */
	protected abstract Function<Table, LazyExpression> initialProvider() throws Exception;
	
	/**
	 * Returns the provider of recursive algorithm recursive step query
	 * @return
	 */
	protected abstract Function<Table, Function<Table, LazyExpression>> recursiveStepProvider() throws Exception;
	
	/**
	 * Returns the provider of postprocessing query
	 * @return
	 */
	protected abstract Function<Table, LazyExpression> postprocessProvider() throws Exception;
	
	public String identificator() {
		return new StringBuilder()
				.append(this.algIdentificator())
				.append(this.parameterString())
				.append("_").append(this.algorithm.getAnnotation(Algorithm.class).value())
				.toString();
	}
	
	/**
	 * Builds the query for data preprocessing
	 * @param iTable
	 * @return
	 */
	public abstract LazyExpression preprocess(LazyExpression iTable);
	
	/**
	 * Buils main query of the experiment
	 * @param iTable
	 * @return
	 */
	public TabularExpression query(Table iTable) {
		return this.getBuilder().buildMainQuery(iTable, this.monitor);
	}
	
	/**
	 * Builds postprocess query of the experiment
	 * @param iTable
	 * @return
	 */
	public TabularExpression postprocess(Table iTable) {
		return this.getBuilder().buildPostprocessQuery(iTable);
	}
	
	/**
	 * Sets all parameters from a parameter map to this query
	 * @param parameterValues
	 */
	public void setQueryParameters(Map<String, String> parameterValues) {
		Class<? extends Queries2> clazz = this.getClass();
		
		for(Method m : clazz.getMethods()) {
			QueryParameter qp = m.getAnnotation(QueryParameter.class);
			if(qp != null) {
				String value = parameterValues.get(qp.value());
				if(value != null) {
					try {
						m.invoke(this, value);
					} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
						throw new RuntimeException(e);
					}
				}
			}
		}
	}
	
	/**
	 * Generates string containing values of all query parameters
	 * @return string
	 */
	public String parameterString() {
		Class<? extends Queries2> clazz = this.getClass();
		
		Map<String, String> parmMap = new TreeMap<String, String>();
		
		for(Method m : clazz.getMethods()) {
			QueryParameterGetter qpg = m.getAnnotation(QueryParameterGetter.class);
			if(qpg != null) {
				try {
					String value = m.invoke(this).toString();
					parmMap.put(qpg.value(), value);
				} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					throw new RuntimeException(e);
				}
			}
		}
		
		return 
			parmMap.entrySet().stream()
				.map(e -> e.toString())
				.reduce((s1, s2) -> new StringBuilder().append(s1).append("_").append(s2).toString())
				.get();
	}
	
	/**
	 * Gets name of the algorithm
	 * @return string
	 */
	public String algorithm() {
		return this.algorithm.getAnnotation(Algorithm.class).value();
	}
	
	/**
	 * List of query classes
	 */
	public static List<Class<? extends Queries2>> QUERIES = Arrays.asList(
			Queries2_Electricity_Week_top.class,
			Queries2_Electricity_Week_tra.class,
			Queries2_Electricity_NoCust_top.class,
			Queries2_Electricity_NoCust_tra.class,
			Queries2_Electricity_Week_Fuzzy_top.class,
			Queries2_Electricity_Week_Fuzzy_tra.class,
			Queries2_Retail_top.class,
			Queries2_Retail_tra.class,
			Queries2_Toloker_top.class,
			Queries2_Toloker_tra.class);
	
	/**
	 * List of queries names
	 */
	public static List<String> QUERIES_NAMES =
			QUERIES.stream().map(clazz -> {
				CallingArg ca = clazz.getAnnotation(CallingArg.class);
				if(ca == null) {
					throw new RuntimeException("Query class " + clazz + " is not decorated with " + CallingArg.class.getName() + " annotation.");
				}
				return ca.value();
			}).collect(Collectors.toList());
	
	
	/**
	 * Helps string with queries names
	 */
	public static final String QUERIES_HELP = QUERIES_NAMES.stream()
			.reduce((s1, s2) -> new StringBuilder().append(s1).append("|").append(s2).toString()).get();
}
