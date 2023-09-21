/**
 * 
 */
package rq.files.io;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import com.opencsv.CSVWriter;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.exceptions.DuplicateHeaderWriteException;
import rq.files.helpers.AttributeSerializer;
import rq.files.helpers.ValueSerializer;

/**
 * @author Mgr. Radomir Skrabal
 *
 */
public class RecordWriter implements Closeable, Flushable {
	
	private final CSVWriter writer;
	private final ValueSerializerContext context;
	private Schema schema;
	
	private RecordWriter(CSVWriter writer, ValueSerializerContext context) {
		this.writer = writer;
		this.context = context;
		this.schema = null;
	}
	
	public static RecordWriter open(OutputStream stream, ValueSerializerContext context) {
		OutputStreamWriter osWriter = new OutputStreamWriter(stream);
		CSVWriter writer = new CSVWriter(osWriter);
		return new RecordWriter(writer, context);
	}
	
	public static RecordWriter open(OutputStream stream) {
		return RecordWriter.open(stream, ValueSerializerContext.DEFAULT);
	}
	
	/**
	 * Makes the csv header
	 * @param schema
	 * @return csv header
	 */
	private String[] serializeHeader(Schema schema) {
		String[] serialized = new String[schema.size()+1];
		int i = 0;
		for(Attribute a : schema) {
			String s = AttributeSerializer.serialize(a);
			serialized[i] = s;
			i++;
		}
		serialized[i] = "rank";
		
		return serialized;
	}
	
	/**
	 * Writes header of the file
	 * @param schema
	 * @throws DuplicateHeaderWriteException
	 */
	public void writeHeader(Schema schema)
		throws DuplicateHeaderWriteException {
		if(this.schema != null) {
			throw new DuplicateHeaderWriteException(this.schema, schema);
		}
		this.schema = schema;
		String[] header = this.serializeHeader(schema);
		this.writer.writeNext(header);
	}
	
	/**
	 * Serializes a single record
	 * @param record
	 * @return serialized record
	 * @throws ClassNotInContextException
	 */
	private String[] serialize(Record record) throws ClassNotInContextException {
		String[] serialized = new String[record.schema.size()+1];
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
		serialized[i] = Double.toString(record.rank);
		
		return serialized;
	}
	
	/**
	 * Writes record into an output stream
	 * @param record
	 * @throws ClassNotInContextException
	 */
	public void writeRecord(Record record) throws ClassNotInContextException {
		if(this.schema == null) {
			try {
				this.writeHeader(record.schema);
			}catch(DuplicateHeaderWriteException e) {
				//Unlikely
				throw new RuntimeException(e);
			}			
		}
		
		String[] line = this.serialize(record);
		this.writer.writeNext(line);
	}

	@Override
	public void close() throws IOException {
		if(this.writer != null) {
			this.writer.flush();
			this.writer.close();
		}
	}

	@Override
	public void flush() throws IOException {
		this.writer.flush();;		
	}

}
