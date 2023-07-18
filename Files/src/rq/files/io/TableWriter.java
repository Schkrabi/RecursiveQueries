package rq.files.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import com.opencsv.CSVWriter;

import rq.common.table.Schema;
import rq.common.table.Table;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.helpers.AttributeSerializer;
import rq.files.helpers.ValueSerializer;
import rq.common.table.Record;
import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.table.Attribute;

/**
 * Persists the table
 * @author Mgr. R.Skrabal
 *
 */
public class TableWriter implements Closeable {
	private final CSVWriter writer;
	private final ValueSerializerContext context;
	
	private TableWriter(CSVWriter writer, ValueSerializerContext context) {
		this.writer = writer;
		this.context = context;
	}
	
	/**
	 * Opens the writer
	 * @param stream 
	 * @return TableWritter instance
	 */
	public static TableWriter open(OutputStream stream) {
		return TableWriter.open(stream, ValueSerializerContext.DEFAULT);
	}
	
	/**
	 * Opens the writer
	 * @param stream
	 * @param context
	 * @return TableWritter instance
	 */
	public static TableWriter open(OutputStream stream, ValueSerializerContext context) {
		OutputStreamWriter osWriter = new OutputStreamWriter(stream);
		CSVWriter writer = new CSVWriter(osWriter);
		return new TableWriter(writer, context);
	}
	
	/**
	 * Writes the table into a file
	 * @param table
	 * @throws ClassNotInContextException 
	 */
	public void write(Table table) throws ClassNotInContextException {
		String[] header = this.serializeHeader(table.schema);
		this.writer.writeNext(header);
		
		for(Record record : table) {
			String[] line = this.serialize(record);
			this.writer.writeNext(line);
		}
	}
	
	/**
	 * Makes the csv header
	 * @param schema
	 * @return csv header
	 */
	private String[] serializeHeader(Schema schema) {
		String[] serialized = new String[schema.size()];
		int i = 0;
		for(Attribute a : schema) {
			String s = AttributeSerializer.serialize(a);
			serialized[i] = s;
			i++;
		}
		
		return serialized;
	}
	
	/**
	 * Serializes a single record
	 * @param record
	 * @return serialized record
	 * @throws ClassNotInContextException
	 */
	private String[] serialize(Record record) throws ClassNotInContextException {
		String[] serialized = new String[record.schema.size()];
		int i = 0;
		for(Attribute a : record.schema) {
			Object value = null;
			try {
				value = record.get(a);
			} catch (AttributeNotInSchemaException e) {
				//Unlikely
				throw new RuntimeException(e);
			}
			
			String s = ValueSerializer.serialize(value, this.context);
			
			serialized[i] = s;
			i++;
		}
		
		return serialized;
	}

	@Override
	public void close() throws IOException {
		if(this.writer != null) {
			this.writer.close();
		}
	}
}
