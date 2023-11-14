package main;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import queries.CallingArg;
import queries.Queries;
import queries.Queries_Electricity_NoCust_UnrTopK;
import queries.Queries_Electricity_NoCust_UnrTra;
import queries.Queries_Electricity_Week_UnrTopK;
import queries.Queries_Electricity_Week_UnrTra;
import queries.Queries_Toloker;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.TabularExpression;
import rq.common.tools.Counter;
import rq.files.io.LazyTable;
import rq.files.io.TableWriter;
import rq.common.interfaces.Table;

/**
 * Main entry point
 * @author Mgr. Radomir Skrabal
 *
 */
public class Main {
	
	private static final String ALGORITHM_UNRESTRICTED = "unrestricted";
	private static final String ALGORITHM_TOPK = "topK";
	private static final String ALGORITHM_TRANSFORMED = "transformed";
	
	private static final List<String> ALGORITHMS = 
			Arrays.asList(
					ALGORITHM_UNRESTRICTED, 
					ALGORITHM_TOPK, 
					ALGORITHM_TRANSFORMED);
	
	private static List<Class<? extends Object>> qlist = Arrays.asList(
			Queries_Electricity_NoCust_UnrTopK.class,
			Queries_Electricity_NoCust_UnrTra.class,
			Queries_Electricity_Week_UnrTopK.class,
			Queries_Electricity_Week_UnrTra.class,
			Queries_Toloker.class);
	
	private static Map<String, BiFunction<Queries.Algorithm, Counter, Queries>> queryMap = new HashMap<String, BiFunction<Queries.Algorithm, Counter, Queries>>();
	
	private static void addQueryFromQueriesClass(Class<? extends Object> clazz) {
		CallingArg ann = clazz.getAnnotation(CallingArg.class);
		if(ann == null) {
			throw new RuntimeException(new StringBuilder()
					.append("Class ")
					.append(clazz.getName())
					.append(" must be annotated with ")
					.append(CallingArg.class.getName())
					.append(" to be used as query source.")
					.toString());
		}
		
		final String factory = "factory";
		try {
			final Method m = clazz.getMethod(factory, Queries.Algorithm.class, Counter.class);
			
			if(	   m == null
				|| !Modifier.isStatic(m.getModifiers())
				|| !Queries.class.isAssignableFrom(m.getReturnType())) {
					throw new RuntimeException(new StringBuilder()
							.append("Class ")
							.append(clazz.getName())
							.append(" must implement static method ")
							.append(factory)
							.append(" to be used as query source.")
							.toString());
				}
				Main.queryMap.put(ann.value(), (Queries.Algorithm a, Counter c) -> {
					try {
						return (Queries)m.invoke(null, a, c);
					} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
						throw new RuntimeException(e);
					}
				});
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * path to input file
	 */
	private final Path path;
	/**
	 * Usage message
	 */
	private static String usage() {
		return new StringBuilder()
				.append("java -jar rq.front.jar <QUERY_NAME> <ALGORITHM> <INPUT FILE PATH>\n")
				.append("ALGORITHM = ").append(ALGORITHMS.stream().reduce((f, s) -> new StringBuilder().append(f).append("|").append(s).toString()).get())
				.append("\n")
				.append("QUERY_NAME = ").append(qlist.stream()
						.map((Class<? extends Object> clazz) -> clazz.getAnnotation(CallingArg.class).value())
						.reduce((f, s) -> new StringBuilder().append(f).append("|").append(s).toString()).get())
				.append("\n")
				.toString();
	}
	
	private static Queries.Algorithm parseAlgorithm(String algStr){
		switch(algStr) {
		case ALGORITHM_UNRESTRICTED:
			return Queries.Algorithm.Unrestricted;
		case ALGORITHM_TOPK:
			return Queries.Algorithm.TopK;
		case ALGORITHM_TRANSFORMED:
			return Queries.Algorithm.Transformed;
		}
		throw new RuntimeException("Algorithm " + algStr + " not recognized");
	}
	
	private static Queries parseQuery(String qStr, Counter counter, Queries.Algorithm algorithm) {
		BiFunction<Queries.Algorithm, Counter, Queries> constructor = queryMap.get(qStr);
		return constructor.apply(algorithm, counter);
	}
	
	private static Main parseArgs(String[] args) {
		Counter counter = new Counter();
		Queries.Algorithm alg = parseAlgorithm(args[1]);
		Queries queries = parseQuery(args[0], counter, alg);
		return new Main(Path.of(args[2]), queries, counter);
	}
	
	/**
	 * Provides queries for the measurement
	 */
	private final Queries queries;
	
	private long loadTime = 0;
	private long queryTime = 0;
	private long outputTime = 0;
	private long preparationTime = 0;
	private Counter recordCounter;
	
	private Main(Path path, Queries queries, Counter recordCounter) {
		this.path = path;
		this.queries = queries;
		this.recordCounter = recordCounter;
	}
	
	/**
	 * Outputs the data to output stream
	 * @param oTable flushed data
	 * @param output a stream
	 */
	private void flushData(Table oTable, OutputStream output) {
		TableWriter writer = null;
		
		try {			
			writer = TableWriter.open(output);
			writer.write(oTable);			
			writer.close();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private OutputStream openDataOutputStream() {
		OutputStream stream = null;
		try {
			stream = new FileOutputStream(this.queries.identificator() + ".csv");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		return stream;
	}
	
	private OutputStream openStatOutputStream() {
		OutputStream stream = null;
		try {
			stream = new FileOutputStream(this.queries.identificator() + ".stat");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		return stream;
	}
	
	/**
	 * Main workhorse method
	 * @param output stream
	 * @throws IOException 
	 */
	private void run() throws IOException {
		
		long start = System.currentTimeMillis();
		
		LazyTable iTable = LazyTable.open(this.path);
		LazyExpression prep = 
				this.queries.prepare(iTable);
		Table prepped = LazyExpression.realizeMapped(prep, 1_000_000);
		
		long end = System.currentTimeMillis();
		this.preparationTime = end - start;
		
		TabularExpression query = 
				this.queries.query(prepped);
		start = System.currentTimeMillis();
		Table oTable = query.eval();
		end = System.currentTimeMillis();
		this.queryTime = end - start;
		
		
		TabularExpression postProcess = 
				this.queries.postprocess(oTable);
		
		start = System.currentTimeMillis();
		oTable = postProcess.eval();
		end = System.currentTimeMillis();
		this.outputTime = end - start;
		
		OutputStream dataOutputStream = this.openDataOutputStream();
		this.flushData(oTable, dataOutputStream);
		dataOutputStream.close();
		
		PrintStream statOutputStream = new PrintStream(this.openStatOutputStream());
		this.outputStatistic(statOutputStream);
		statOutputStream.close();
		
		System.out.println("Processing finished.\n");
	}
	
	private void outputStatistic(PrintStream output) {
		output.println("Data load time(ms): " + this.loadTime);
		output.println("Data preparation time(ms): " + this.preparationTime);
		output.println("Query execution time(ms): " + this.queryTime);
		output.println("Postprocess time(ms): " + this.outputTime);
		output.println("Tuples inserted: " + this.recordCounter.count());
	}

	/**
	 * Main entry point
	 * @param args program arguments
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if(!(args.length == 3
				&& (args[0] instanceof String))){
			System.out.println(usage());
			return;
		}
		Main.qlist.forEach(c -> Main.addQueryFromQueriesClass(c));
		
		Main me = parseArgs(args);
		me.run();
	}

}
