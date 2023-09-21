package rq.files.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.opencsv.exceptions.CsvValidationException;

import rq.common.table.Schema;
import rq.common.table.MemoryTable;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.exceptions.ColumnOrderingNotInitializedException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.table.Record;

/**
 * Class for reading table from a file
 * @author Mgr. R.Skrabal
 *
 */
public class TableReader implements Closeable{	
	private RecordReader recordReader;
	
	private TableReader(RecordReader recordReader) {
		this.recordReader = recordReader;
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
		RecordReader recordReader = RecordReader.open(stream, context);
		return new TableReader(recordReader);
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
	 * @throws ColumnOrderingNotInitializedException 
	 * @throws ClassNotInContextException
	 */
	public MemoryTable read() throws CsvValidationException, ClassNotFoundException, DuplicateAttributeNameException,
			IOException, ColumnOrderingNotInitializedException, ClassNotInContextException, TableRecordSchemaMismatch {
		Schema schema = this.recordReader.schema();
		MemoryTable table = new MemoryTable(schema);

		Record record = this.recordReader.next();
		while (record != null) {
			table.insert(record);
			record = this.recordReader.next();
		}

		return table;
	}

	@Override
	public void close() throws IOException {
		this.recordReader.close();
	}
}
