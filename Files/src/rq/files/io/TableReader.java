package rq.files.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;
import java.util.ArrayList;
import java.io.InputStreamReader;
import java.io.Reader;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import rq.common.table.Schema;
import rq.common.table.Table;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.exceptions.ColumnOrderingNotInitializedException;
import rq.files.helpers.AttributeParser;
import rq.files.helpers.ValueParser;
import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.table.Record;
import rq.common.table.Attribute;

/**
 * Class for reading table from a file
 * @author Mgr. R.Skrabal
 *
 */
public class TableReader implements Closeable{
	private final CSVReader reader;
	private Schema schema = null;
	private List<Attribute> columnOrder = null;
	private ValueParserContext context;
	private Function<String[], Double> rankParser = (String[] s) -> 1.0d;
	
	private TableReader(CSVReader reader, ValueParserContext context) {
		this.reader = reader;
		this.context = context;
	}
	
	public static TableReader open(Path path)
		throws IOException {
		return TableReader.open(path, ValueParserContext.DEFAULT);
	}
	
	public static TableReader open(Path path, ValueParserContext context) 
		throws IOException {
		InputStream stream = Files.newInputStream(path);
		return TableReader.open(stream, context);
	}
	
	public static TableReader open(InputStream stream, ValueParserContext context) 
		throws IOException {
		Reader sReader = new InputStreamReader(stream);
		CSVReader csvReader = new CSVReader(sReader);
		return new TableReader(csvReader, context);
	}
	
	public static TableReader open(InputStream stream)
		throws IOException {
		return TableReader.open(stream, ValueParserContext.DEFAULT);
	}
	
	public static TableReader open(String path)
		throws IOException {
		Path p = Paths.get(path);
		return TableReader.open(p);
	}
	
	/**
	 * Reads the table
	 * @return the table instance
	 * @throws CsvValidationException
	 * @throws IOException
	 * @throws TableRecordSchemaMismatch
	 * @throws ClassNotFoundException
	 * @throws DuplicateAttributeNameException
	 * @throws ClassNotInContextException
	 */
	public Table read() 
			throws CsvValidationException, 
			IOException, 
			TableRecordSchemaMismatch, 
			ClassNotFoundException, 
			DuplicateAttributeNameException, ClassNotInContextException{
		String[] headerLine = this.reader.readNext();
		Schema schema = this.parseSchema(headerLine);
		Table table = new Table(schema);
		
		String[] line  = this.reader.readNext();
		while(line != null) {
			Record record;
			try {
				record = this.parseLine(line);
			} catch (ColumnOrderingNotInitializedException e) {
				// Unlikely
				throw new RuntimeException(e);
			}
			table.insert(record);
			line = this.reader.readNext();
		}
		return table;
	}
	
	/**
	 * Parses header of the csv file as a Schema
	 * @param headers
	 * @return
	 * @throws ClassNotFoundException 
	 * @throws DuplicateAttributeNameException 
	 */
	private Schema parseSchema(String[] headers) throws ClassNotFoundException, DuplicateAttributeNameException {
		this.columnOrder = new ArrayList<Attribute>();
		
		for(int i = 0; i < headers.length; i++) {
			String s = headers[i];
			if(!s.equals("rank")) {
				this.columnOrder.add(AttributeParser.parse(s));
			} else {
				final int j = i;
				this.rankParser = (String[] r) -> Double.parseDouble(r[j]);
				this.columnOrder.add(null);
			}
		}
		
		Schema s = Schema.factory(this.columnOrder.stream().filter(x -> x != null).toList());
		this.schema = s;
		return s;
	}
	
	/**
	 * Parses a single line
	 * @param line string data
	 * @return parsed record
	 * @throws ColumnOrderingNotInitializedException
	 * @throws ClassNotInContextException
	 */
	private Record parseLine(String[] line) 
		throws ColumnOrderingNotInitializedException, ClassNotInContextException{
		if(this.columnOrder == null) {
			throw new ColumnOrderingNotInitializedException();
		}
		List<Record.AttributeValuePair> l = new ArrayList<Record.AttributeValuePair>();
		int i = 0;
		for(Attribute a : this.columnOrder) {
			if(a!= null) {
				Object o = ValueParser.parse(a, line[i], this.context);
				Record.AttributeValuePair pair = new Record.AttributeValuePair(a, o);
				l.add(pair);
			}
			i++;
		}
		
		Record r;
		try {
			r = Record.factory(
					this.schema, 
					l, 
					this.rankParser.apply(line));
		} catch (TypeSchemaMismatchException | AttributeNotInSchemaException e) {
			// Unlikely
			throw new RuntimeException(e);
		}
		
		return r;
	}

	@Override
	public void close() throws IOException {
		this.reader.close();		
	}
}
