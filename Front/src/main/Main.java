package main;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import algorithmBuilder.AlgorithmBuilder;
import algorithmBuilder.AlgorithmBuilderTransformed_topKPruning;
import annotations.BuildsAlgorithm;
import annotations.CallingArg;
import queries.Queries2;
import rq.common.algorithms.LazyRecursive;
import rq.common.annotations.Algorithm;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.TabularExpression;
import rq.common.tools.AlgorithmMonitor;
import rq.files.io.LazyTable;
import rq.files.io.TableWriter;
import rq.common.interfaces.Table;

/**
 * Main entry point
 * @author Mgr. Radomir Skrabal
 *
 */
public class Main {	
	private static final String STATHEADER = "statheader";
	private static final String ARGS_FILE_SWITCH = "-argsFile";
	
	/**
	 * path to input file
	 */
	private final Path path;
	/**
	 * Usage message
	 */
	private static String usage() {
		return new StringBuilder()
				.append("Runs the experiment\n")
				.append("java -jar rq.front.jar (QUERY_NAME) (ALGORITHM) (INPUT FILE PATH) [QUERY_ARGS...]\n")
				.append("ALGORITHM = ").append(AlgorithmBuilderTransformed_topKPruning.ALGORTIHMS_HELP)
				.append("\n")
				.append("QUERY_NAME = ").append(Queries2.QUERIES_HELP)
				.append("\n\n")
				.append("Prints header for the .stat files concatenation\n")
				.append("java -jar rq.front.jar ").append(STATHEADER)
				.toString();
	}
	
	@SuppressWarnings("unchecked")
	private static Class<? extends LazyRecursive> parseAlgorithm(String algStr){
		Class<?> clazz = AlgorithmBuilder.BUILDERS.stream()
				.filter(c -> {
					BuildsAlgorithm ba = c.getAnnotation(BuildsAlgorithm.class);
					
					if(ba != null) {
						Algorithm a = ba.value().getAnnotation(Algorithm.class);
						return a.value().equals(algStr);
					}
					return false;
				})
				.map(c -> {
					BuildsAlgorithm ba = c.getAnnotation(BuildsAlgorithm.class);
					return ba.value();
				}).findAny().get();
		
		return (Class<? extends LazyRecursive>)clazz;					
	}
	
	private static Queries2 parseQuery(String qStr, AlgorithmMonitor monitor, Class<? extends LazyRecursive> algorithm) {
		try {
			Class<? extends Queries2> clazz;
				
			clazz = Queries2.QUERIES.stream().filter(c -> {
				CallingArg ca = c.getAnnotation(CallingArg.class);
				return ca.value().equals(qStr);
			}).findAny().get();
			Constructor<? extends Queries2> cons = null;
			cons = clazz.getConstructor(Class.class, AlgorithmMonitor.class);
			Queries2 instance = cons.newInstance(algorithm, monitor);
			return instance;
		} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	private static Main parseArgs(String[] args) {
		AlgorithmMonitor monitor = new AlgorithmMonitor();
		Class<? extends LazyRecursive> alg = parseAlgorithm(args[1]);
		Queries2 queries = parseQuery(args[0], monitor, alg);
		
		Map<String, String> queryArgsMap = null;
		if(args.length == 5 && args[3].equals(ARGS_FILE_SWITCH)) {
			queryArgsMap = Main.parseQueryArgFile(Paths.get(args[4]));
		}
		else {
			queryArgsMap = new HashMap<String, String>();
			for(int i = 3; i < args.length; i++) {
				parseQueryArg(args[i], queryArgsMap);
			}
		}
		
		return new Main(Path.of(args[2]), queries, monitor, queryArgsMap);
	}
	
	/**
	 * Provides queries for the measurement
	 */
	private final Queries2 queries;
	
	private long loadTime = 0;
	private long queryTime = 0;
	private long postprocessTime = 0;
	private long preparationTime = 0;
	private AlgorithmMonitor monitor = null;
	
	private Main(Path path, Queries2 queries, AlgorithmMonitor monitor, Map<String, String> queryArgsMap) {
		this.path = path;
		this.queries = queries;
		this.queries.setQueryParameters(queryArgsMap);
		this.monitor = monitor;
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
				this.queries.preprocess(iTable);
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
		this.postprocessTime = end - start;
		
		OutputStream dataOutputStream = this.openDataOutputStream();
		this.flushData(oTable, dataOutputStream);
		dataOutputStream.close();
		
		PrintStream statOutputStream = new PrintStream(this.openStatOutputStream());
		this.outputStatistic(statOutputStream);
		statOutputStream.close();
		
		System.out.println("Processing finished.\n");
	}
	
	private static void statHeader() {
		String s = new StringBuilder()
				.append("query")
				.append(",")
				.append("algorithm")
				.append(",")
				.append("load-time")
				.append(",")
				.append("preprocess-time")
				.append(",")
				.append("query-time")
				.append(",")
				.append("postprocess-time")
				.append(",")
				.append("tuples-generated")
				.append(",")
				.append("result-candidates")
				.append(",")
				.append("query-parameters")
				.append("\n")
				.toString();
		System.out.print(s);
	}
	
	private void outputStatistic(PrintStream output) {
		String s = new StringBuilder()
				.append(this.queries.getClass().getAnnotation(CallingArg.class).value())
				.append(",")
				.append(this.queries.algorithm())
				.append(",")
				.append(this.loadTime)
				.append(",")
				.append(this.preparationTime)
				.append(",")
				.append(this.queryTime)
				.append(",")
				.append(this.postprocessTime)
				.append(",")
				.append(this.monitor.generatedTuples.count())
				.append(",")
				.append(this.monitor.resultCandidates.count())
				.append(",")
				.append(this.queries.parameterString())
				.toString();
		
		output.println(s);
	}
	
	private static boolean validateArgs(String[] args) {
		return Files.exists(Path.of(args[2]))
				&& Queries2.QUERIES_NAMES.contains(args[0])
				&& AlgorithmBuilder.ALGORITHM_LIST.contains(args[1]);
	}
	
	private static Map<String, String> parseQueryArg(String arg, Map<String, String> map){
		String[] strs = arg.split("=");
		map.put(strs[0], strs[1]);
		return map;
	}
	
	private static Map<String, String> parseQueryArgFile(Path path){
		try {
			Scanner scanner = new Scanner(path.toFile());
			Map<String, String> argMap = new HashMap<String, String>();
			
			while(scanner.hasNextLine()) {
				argMap = parseQueryArg(scanner.nextLine(), argMap);
			}
			return argMap;
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Main entry point
	 * @param args program arguments
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {	
		if(args.length == 1 && args[0].equals(STATHEADER)) {
			statHeader();
			return;
		}
		
		if(!(args.length >= 3
				&& validateArgs(args))){
			System.out.println(usage());
			return;
		}		
		
		Main me = parseArgs(args);
		me.run();
	}

}
