package main;

import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;

import rq.common.table.Table;
import rq.common.table.TabularExpression;
import rq.files.io.TableReader;
import rq.files.io.TableWriter;

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
	
	private Main(Path path) {
		this.path = path;
	}
	
	/**
	 * Constructs the query to execute
	 * @param iTable
	 * @return
	 */
	private TabularExpression query(Table iTable) {
		//return iTable;
		return Queries.electricityLoadDiagrams_repeatingPeaks("MT_124", 200.0d, Duration.ofHours(1), iTable);
	}
	
	/**
	 * Loads data from input file
	 * @return table instance
	 */
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
	 */
	private void run(OutputStream output) {
		Table iTable = this.loadData();
		TabularExpression query = this.query(iTable);
		Table oTable = query.eval();
		this.flushData(oTable, output);
	}

	/**
	 * Main entry point
	 * @param args program arguments
	 */
	public static void main(String[] args) {
		if(!(args.length == 1
				&& (args[0] instanceof String))){
			System.out.println(USAGE);
			return;
		}
		
		Main me = new Main(Path.of(args[0]));
		me.run(System.out);
	}

}
