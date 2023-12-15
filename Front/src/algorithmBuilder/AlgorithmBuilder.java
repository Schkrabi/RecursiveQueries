package algorithmBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import annotations.BuildsAlgorithm;
import rq.common.algorithms.LazyRecursive;
import rq.common.annotations.Algorithm;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.tools.AlgorithmMonitor;

public abstract class AlgorithmBuilder {

	protected final Function<Table, LazyExpression> initialExpressionProvider;
	protected final Function<Table, Function<Table, LazyExpression>> recursiveExpressionProvider;
	protected final Function<Table, LazyExpression> postprocessProvider;
	protected final int k;
	
	protected AlgorithmBuilder(
			Function<Table, LazyExpression> initialExpressionProvider,
			Function<Table, Function<Table, LazyExpression>> recursiveExpressionProvider,
			Function<Table, LazyExpression> postprocessProvider,
			Integer k) {
		this.initialExpressionProvider = initialExpressionProvider;
		this.recursiveExpressionProvider = recursiveExpressionProvider;
		this.postprocessProvider = postprocessProvider;
		this.k = k;
	}
	
	public abstract TabularExpression buildMainQuery(Table iTable, AlgorithmMonitor monitor);
	public abstract TabularExpression buildPostprocessQuery(Table iTable);
	
	/**
	 * Algorithms for factory method
	 */
	public static final List<Class<? extends AlgorithmBuilder>> BUILDERS =
			Arrays.asList(
					AlgorithmBuilderUnrestricted.class,
					AlgorithmBuilderTransformed.class,
					AlgorithmBuilderTransformed_topKPruning.class,
					AlgorithmBuilderTopK.class);
	
	/**
	 * List of algorithm names
	 */
	public static final List<String> ALGORITHM_LIST = 
			BUILDERS.stream()
			.map(clazz -> {
				BuildsAlgorithm ba = clazz.getAnnotation(BuildsAlgorithm.class);
				if(ba == null) {
					throw new RuntimeException("Algorithm builder " + clazz.getName() + " must be decorated with " + BuildsAlgorithm.class.getName() + " annotation");
				}
				
				Algorithm a = ba.value().getAnnotation(Algorithm.class);
				
				if(a == null) {
					throw new RuntimeException("Algorithm " + ba.getClass().getName() + " must be decorated with " + Algorithm.class.getName() + " annotation");
				}
				
				return a.value();
			})
			.filter(s -> !s.equals(""))
			.collect(Collectors.toList());
	
	/**
	 * String with algorithm names help
	 */
	public static final String ALGORTIHMS_HELP = 
				ALGORITHM_LIST.stream()
				.reduce((s1, s2) -> new StringBuilder().append(s1).append("|").append(s2).toString())
			.get();
	
	/**
	 * Constructs instance of algorithm builder from the arguments
	 * @param algorithm
	 * @param initialExpressionProvider
	 * @param recursiveExpressionProvider
	 * @param postprocessProvider
	 * @return
	 */
	public static AlgorithmBuilder construct(
			Class<? extends LazyRecursive> algorithm,
			Function<Table, LazyExpression> initialExpressionProvider,
			Function<Table, Function<Table, LazyExpression>> recursiveExpressionProvider,
			Function<Table, LazyExpression> postprocessProvider,
			int k) {
		for(Class<? extends AlgorithmBuilder> c : BUILDERS) {
			BuildsAlgorithm ba = c.getAnnotation(BuildsAlgorithm.class);
			if(ba != null && ba.value().equals(algorithm)) {
				try {	
					Constructor<? extends AlgorithmBuilder> cons = 
							c.getConstructor(
									Function.class, 
									Function.class, 
									Function.class,
									Integer.class);
					AlgorithmBuilder builder = cons.newInstance(
							initialExpressionProvider, 
							recursiveExpressionProvider, 
							postprocessProvider,
							k);
					return builder;
				} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					throw new RuntimeException(e);
				}
			}
		}
		
		throw new RuntimeException("No algorithm builder found for " + algorithm.getName());
	}
}
