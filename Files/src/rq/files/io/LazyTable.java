/**
 * 
 */
package rq.files.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import rq.common.table.Schema;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.SchemaProvider;
import rq.common.table.Record;

/**
 * @author Mgr. Radomir Skrabal
 *
 */
public class LazyTable implements Closeable, SchemaProvider, LazyExpression {
	private RecordReader reader;
	
	private LazyTable(RecordReader reader) {
		this.reader = reader;
	}
	
	public static LazyTable open(Path path)
			throws IOException {
			return new LazyTable(RecordReader.open(path, ValueParserContext.DEFAULT));
	}
	
	public static LazyTable open(InputStream stream)
			throws IOException {
		return new LazyTable(RecordReader.open(stream, ValueParserContext.DEFAULT));
	}
	
	@Override
	public void close() throws IOException {
		if(this.reader != null) {
			this.reader.close();
		}
	}

	@Override
	public Record next() {
		try {
			return reader.next();
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Schema schema() {
		try {
			return reader.schema();
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
}
