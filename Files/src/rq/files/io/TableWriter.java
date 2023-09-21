package rq.files.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import rq.common.interfaces.Table;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.exceptions.DuplicateHeaderWriteException;
import rq.common.table.Record;

/**
 * Persists the table
 * @author Mgr. R.Skrabal
 *
 */
public class TableWriter implements Closeable {
	private final RecordWriter recordWriter;
	
	private TableWriter(RecordWriter recordWriter) {
		this.recordWriter = recordWriter;
	}
	
	/**
	 * Opens the writer
	 * @param stream 
	 * @return TableWritter instance
	 */
	public static TableWriter open(OutputStream stream) {
		return new TableWriter(RecordWriter.open(stream));
	}
	
	/**
	 * Opens the writer
	 * @param stream
	 * @param context
	 * @return TableWritter instance
	 */
	public static TableWriter open(OutputStream stream, ValueSerializerContext context) {
		return new TableWriter(RecordWriter.open(stream, context));
	}
	
	/**
	 * Writes the table into a file
	 * @param oTable
	 * @throws ClassNotInContextException 
	 * @throws IOException 
	 * @throws DuplicateHeaderWriteException 
	 */
	public void write(Table oTable) throws ClassNotInContextException, IOException, DuplicateHeaderWriteException {
		this.recordWriter.writeHeader(oTable.schema());
		for(Record record : oTable) {
			this.recordWriter.writeRecord(record);
		}
		this.recordWriter.flush();
	}

	@Override
	public void close() throws IOException {
		if(this.recordWriter != null) {
			this.recordWriter.flush();
			this.recordWriter.close();
		}
	}
}
