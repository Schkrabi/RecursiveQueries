/**
 * 
 */
package rq.files.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.exceptions.ColumnOrderingNotInitializedException;
import rq.files.helpers.AttributeParser;
import rq.files.helpers.ValueParser;

/**
 * Reads record from a csv file
 * @author Mgr. Radomir Skrabal
 *
 */
public class RecordReader implements Closeable {
	
	private CSVReader reader;
	private Schema schema = null;
	private List<Attribute> columnOrder = null;
	private ValueParserContext context;
	private Function<String[], Double> rankParser = (String[] s) -> 1.0d;
	
	private RecordReader(CSVReader reader, ValueParserContext context) {
		this.reader = reader;
		this.context = context;
	}
	
	public static RecordReader open(Path path)
			throws IOException {
			return RecordReader.open(path, ValueParserContext.DEFAULT);
	}
	
	public static RecordReader open(Path path, ValueParserContext context) 
		throws IOException {
		InputStream stream = Files.newInputStream(path);
		return RecordReader.open(stream, context);
	}
	
	public static RecordReader open(InputStream stream, ValueParserContext context) 
		throws IOException {
		Reader sReader = new InputStreamReader(stream);
		CSVReader csvReader = new CSVReader(sReader);
		return new RecordReader(csvReader, context);
	}
	
	public static RecordReader open(InputStream stream)
		throws IOException {
		return RecordReader.open(stream, ValueParserContext.DEFAULT);
	}
	
	public static RecordReader open(String path)
		throws IOException {
		Path p = Paths.get(path);
		return RecordReader.open(p);
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
		
		Schema s = Schema.factory(this.columnOrder.stream().filter(x -> x != null).collect(Collectors.toList()));
		this.schema = s;
		return s;
	}
	
	public Schema schema() throws CsvValidationException, ClassNotFoundException, DuplicateAttributeNameException, IOException {
		if(this.schema == null) {
			this.schema = this.parseSchema(this.reader.readNext());
		}
		return schema;
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
	
	public Record next() throws CsvValidationException, ClassNotFoundException, DuplicateAttributeNameException,
			IOException, ColumnOrderingNotInitializedException, ClassNotInContextException {
		if(this.schema == null) {
			this.schema = this.schema();
		}
		String[] line = this.reader.readNext();
		if(line == null) {
			return null;
		}
		Record record = this.parseLine(line);
		return record;
	}

	@Override
	public void close() throws IOException {
		this.reader.close();
	}

}
