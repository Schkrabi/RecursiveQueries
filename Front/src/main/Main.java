package main;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Month;

import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.TabularExpression;
import rq.common.types.Str10;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.io.LazyTable;
import rq.files.io.RecordWriter;
import rq.files.io.TableReader;
import rq.files.io.TableWriter;
import rq.common.interfaces.Table;

/**
 * Main entry point
 * @author Mgr. Radomir Skrabal
 *
 */
public class Main {
	
	/**
	 * path to input file
	 */
	private final Path path;
	/**
	 * Usage message
	 */
	private static final String USAGE = "java -jar rq.front.jar <INPUT FILE PATH>";
	
	private long loadTime = 0;
	private long queryTime = 0;
	private long outputTime = 0;
	private long preparationTime = 0;
	
	private Main(Path path) {
		this.path = path;
	}
	
	@SuppressWarnings("unused")
	private TabularExpression prepQuery(Table iTable) {
		return iTable;
		//return Queries.electricityLoadDiagrams_CustTresholdAndPeriod("MT_124", 200.0d, Duration.ofHours(1), iTable);
	}
	
	/**
	 * Constructs the query to execute
	 * @param iTable
	 * @return
	 */
	@SuppressWarnings("unused")
	private TabularExpression query(Table iTable) {
		return iTable;
		//return Queries.electricityLoadDiagrams_repeatingPeaks_Mapped_Lazy(iTable);
//		try {
//			return Queries.electricityLoadDiagrams_benchmark(iTable);
//		} catch (AttributeNotInSchemaException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return null;
		
	}
	
	/**
	 * Loads data from input file
	 * @return table instance
	 */
	@SuppressWarnings("unused")
	private Table loadData() {
		TableReader reader = null;
		Table iTable = null;
		
		try {
			reader = TableReader.open(this.path);
			iTable = reader.read();
			reader.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		return iTable;
	}
	
	/**
	 * Outputs the data to output stream
	 * @param oTable flushed data
	 * @param output a stream
	 */
	private void flushData(Table oTable, OutputStream output) {
		TableWriter writer = null;
		
		try {
			this.outputStatistic(System.out);
			
			writer = TableWriter.open(output);
			writer.write(oTable);			
			writer.close();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Main workhorse method
	 * @param output stream
	 * @throws IOException 
	 */
	private void run(OutputStream output) throws IOException {
		
		long start = System.currentTimeMillis();
		
		LazyTable iTable = LazyTable.open(this.path);
		LazyExpression prep = 
				Queries_Electricity.instance()
					.caseWeekPrepare(iTable);
					//.caseNoCustPrepare(iTable);
				//Queries_Toloker.instance().prepare(iTable);
		Table prepped = LazyExpression.realizeMapped(prep, 1_000_000);
		
		long end = System.currentTimeMillis();
		this.preparationTime = end - start;
		
		TabularExpression query = 
				Queries_Electricity.instance()
					.caseWeekUnrestricted(prepped);
					//.caseWeekTopK(prepped);
					//.caseWeekTransformed(prepped);
					//.caseNoCustUnrestricted(prepped);
					//.caseNoCustTopK(prepped);
					//.caseNoCustTransformed(prepped);
				//Queries_Toloker.instance()
				//	.unrestricted(prepped);
		start = System.currentTimeMillis();
		Table oTable = query.eval();
		end = System.currentTimeMillis();
		this.queryTime = end - start;
		
		start = System.currentTimeMillis();
		this.flushData(oTable, output);
		end = System.currentTimeMillis();
		this.outputTime = end - start;
	}
	
	private void outputStatistic(PrintStream output) {
		output.println("Data load time(ms): " + this.loadTime);
		output.println("Data preparation time(ms): " + this.preparationTime);
		output.println("Query execution time(ms): " + this.queryTime);
		output.println("Data output time(ms): " + this.outputTime);
	}

	/**
	 * Main entry point
	 * @param args program arguments
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if(!(args.length == 1
				&& (args[0] instanceof String))){
			System.out.println(USAGE);
			return;
		}
		
		Main me = new Main(Path.of(args[0]));
		me.run(System.out);
//		try {
//			me.runLazy(System.out);
//		} catch (ClassNotInContextException | IOException e) {
//			e.printStackTrace();
//		}
	}

}
