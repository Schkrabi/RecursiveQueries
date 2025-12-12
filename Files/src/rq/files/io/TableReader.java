package rq.files.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import com.opencsv.exceptions.CsvValidationException;

import rq.common.table.Schema;
import rq.common.table.FileMappedTable;
import rq.common.table.MemoryTable;
import rq.files.exceptions.ColumnOrderingNotInitializedException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.table.Record;
import rq.common.interfaces.Table;
import rq.common.io.contexts.ClassNotInContextException;
import rq.common.io.contexts.ValueParserContext;

/**
 * Class for reading table from a file
 * @author Mgr. R.Skrabal
 *
 */
public class TableReader implements Closeable{	
	private RecordReader recordReader;
	private final Function<Schema, Table> tableSupplier;
	
	private TableReader(RecordReader recordReader, Function<Schema, Table> schemaSupplier) {
		this.recordReader = recordReader;
		this.tableSupplier = schemaSupplier;
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
		return new TableReader(recordReader, (Schema s) -> new MemoryTable(s));
	}
	
	public static TableReader open(InputStream stream)
		throws IOException {
		return TableReader.open(stream, ValueParserContext.DEFAULT);
	}
	
	public static TableReader openMappedToFile(InputStream stream, ValueParserContext context, int capacity)
		throws IOException {
		RecordReader recordReader = RecordReader.open(stream, context);
		return new TableReader(recordReader, (Schema s) -> {
			try {
				return FileMappedTable.factory(s, capacity);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}
	
	public static TableReader openMappedToFile(InputStream stream, int capacity)
		throws IOException {
		return TableReader.openMappedToFile(stream, ValueParserContext.DEFAULT, capacity);
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
	public Table read() throws CsvValidationException, ClassNotFoundException, DuplicateAttributeNameException,
			IOException, ColumnOrderingNotInitializedException, ClassNotInContextException, TableRecordSchemaMismatch {
		Schema schema = this.recordReader.schema();
		Table table = this.tableSupplier.apply(schema);

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
