/**
 * 
 */
package rq.common.table;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Queue;
import java.util.LinkedList;

import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DomainNotByteSerializeableException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.interfaces.ByteArraySerializable;
import rq.common.interfaces.Table;

/**
 * Table mapped to a file
 * @author Mgr. Radomir Skrabal
 *
 */
public class FileMappedTable implements Closeable, Table {
	
	private static final String FILE_ACCESS_MODE = "rw";
	private static final int DEFAULT_RECORD_CAPACITY = 100;
	
	private final Schema schema;
	
	private final File tmpFile;
	private final RandomAccessFile file;
	private final FileChannel channel;
	private final MappedByteBuffer mappedBuffer;
	
	private Queue<Integer> vacantPositions = new LinkedList<Integer>();
	private int schemaByteSize;
	
	private FileMappedTable(Schema schema,
			File tmpFile,
			RandomAccessFile file,
			FileChannel channel,
			MappedByteBuffer mappedBuffer,
			int schemaByteSize) {
		this.schema = schema;
		this.tmpFile = tmpFile;
		this.file = file;
		this.channel = channel;
		this.mappedBuffer = mappedBuffer;
		this.schemaByteSize = schemaByteSize;
	}
	
	/**
	 * Factory method
	 * @param schema schema of the table
	 * @return a table with default capacity
	 * @throws IOException
	 */
	public static FileMappedTable factory(Schema schema) throws IOException {
		return FileMappedTable.factory(schema, DEFAULT_RECORD_CAPACITY);
	}
	
	/**
	 * Fatory method
	 * @param schema of the table
	 * @param recordCapacity number of the records this table can obtain
	 * @return a table instance 
	 * @throws IOException
	 */
	public static FileMappedTable factory(Schema schema, int recordCapacity) throws IOException {
		List<Attribute> failAttrs = FileMappedTable.validateSchema(schema);
		if(!failAttrs.isEmpty()) {
			throw new DomainNotByteSerializeableException(failAttrs.stream().findAny().get().domain);
		}		
		
		File tmpFile = File.createTempFile("rq.table.", ".bin");
		RandomAccessFile file = new RandomAccessFile(tmpFile, FILE_ACCESS_MODE);
		int schemaByteSize = FileMappedTable.schemaByteSize(schema);
		int fileLen = schemaByteSize * recordCapacity;
		file.setLength(fileLen);
		FileChannel channel = file.getChannel();
		long fileSize = channel.size();
		MappedByteBuffer mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
		
		return new FileMappedTable(
				schema,
				tmpFile,
				file,
				channel,
				mappedBuffer,
				schemaByteSize);
	}

	@Override
	public Table eval() {
		return this;
	}

	@Override
	public Schema schema() {
		return this.schema;
	}
	
	/**¨
	 * Returns list of attributes in schema that cannot be saved in mapped file
	 * @param schema 
	 * @return list of attributes
	 */
	private static List<Attribute> validateSchema(Schema schema) {
		return schema.stream()
				.filter(a -> !(a.domain.equals(Double.class) 
							|| a.domain.equals(Float.class) 
							|| a.domain.equals(Integer.class) 
							|| a.domain.equals(Long.class) 
							|| a.domain.equals(Short.class)
							|| ByteArraySerializable.class.isAssignableFrom(a.domain)))
				.collect(Collectors.toList());
	}
	
	protected static int schemaByteSize(Schema schema) {
		int recordSize = schema.stream()
				.map(a -> {
						if(a.domain.equals(Double.class)) return Double.BYTES;
						else if(a.domain.equals(Float.class)) return Float.BYTES;
						else if(a.domain.equals(Integer.class)) return Integer.BYTES;
						else if(a.domain.equals(Long.class)) return Long.BYTES;
						else if(a.domain.equals(Short.class)) return Short.BYTES;
						else if(ByteArraySerializable.class.isAssignableFrom(a.domain))
							try {
								return ((ByteArraySerializable)a.domain.getConstructor().newInstance()).byteArraySize();
							} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
									| InvocationTargetException | NoSuchMethodException | SecurityException e) {
								throw new RuntimeException(e);
							}
						throw new RuntimeException("Cannot determine length.");
					})
				.reduce(0, (a, b) -> a + b) + Double.BYTES;
		return recordSize;
	}
	
	/**
	 * Writes value into the file
	 * @param object value
	 */
	private void writeValue(Object object) {
		if(object instanceof Double) {
			this.mappedBuffer.putDouble((double)object);
			return;
		}
		if(object instanceof Float) {
			this.mappedBuffer.putFloat((float)object);
			return;
		}
		if(object instanceof Integer) {
			this.mappedBuffer.putInt((int)object);
			return;
		}
		if(object instanceof Long) {
			this.mappedBuffer.putLong((long)object);
			return;
		}
		if(object instanceof Short) {
			this.mappedBuffer.putShort((short)object);
			return;
		}
		if(ByteArraySerializable.class.isAssignableFrom(object.getClass())) {
			ByteArraySerializable bas = (ByteArraySerializable)object;
			this.mappedBuffer.put(bas.toBytes());
			return;
		}
		throw new DomainNotByteSerializeableException(object.getClass());
	}
	
	/**
	 * Writes recoed into the file
	 * @param record
	 */
	private void writeRecord(Record record) {
		this.schema.stream()
			.forEach(a -> this.writeValue(record.getNoThrow(a)));
		this.mappedBuffer.putDouble(record.rank);
	}
	
	/**
	 * Inserts recoed into the table
	 * @param record
	 * @return
	 */
	@Override
	public boolean insert(Record record) {
		int savedPos = this.mappedBuffer.position();
		if(!this.vacantPositions.isEmpty()) {
			this.mappedBuffer.position(this.vacantPositions.poll());
		}
		this.writeRecord(record);
		if(savedPos > this.mappedBuffer.position()) {
			this.mappedBuffer.position(savedPos);
		}
		return true;
	}
	
	/**
	 * Inserts record into the database 
	 * @param values named values
	 * @param rank rank of the record
	 * @return true if inserted, false otherwise
	 * @throws AttributeNotInSchemaException 
	 * @throws TypeSchemaMismatchException 
	 * @throws TableRecordSchemaMismatch 
	 */
	@Override
	public boolean insert(Collection<Record.AttributeValuePair> values, double rank)
			throws TypeSchemaMismatchException, AttributeNotInSchemaException, TableRecordSchemaMismatch {
		Record r = Record.factory(this.schema, values, rank);
		return this.insert(r);
	}
	
	/**
	 * Reads value from a file
	 * @param attribute
	 * @return value
	 */
	private Object readValue(Attribute attribute) {
		if(attribute.domain.isAssignableFrom(Double.class)) {
			return this.mappedBuffer.getDouble();
		}
		if(attribute.domain.isAssignableFrom(Float.class)) {
			return this.mappedBuffer.getFloat();
		}
		if(attribute.domain.isAssignableFrom(Integer.class)) {
			return this.mappedBuffer.getInt();
		}
		if(attribute.domain.isAssignableFrom(Long.class)) {
			return this.mappedBuffer.getLong();
		}
		if(attribute.domain.isAssignableFrom(Short.class)) {
			return this.mappedBuffer.getShort();
		}
		if(ByteArraySerializable.class.isAssignableFrom(attribute.domain)) {
			ByteArraySerializable value = null;
			try {
				value = (ByteArraySerializable)attribute.domain.getConstructor().newInstance();
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException e) {
				throw new RuntimeException(e);
			}
			byte[] bytes = new byte[value.byteArraySize()];
			this.mappedBuffer.get(bytes);
			value.fromBytes(bytes);
			return value;
		}
		throw new DomainNotByteSerializeableException(attribute.domain);
	}
	
	/**
	 * Reads recod from a file
	 * @return record
	 */
	private Record readRecord() {
		List<Record.AttributeValuePair> vls = 
			this.schema.stream()
				.map(a -> new Record.AttributeValuePair(a, this.readValue(a)))
				.collect(Collectors.toList());
		
		double rank = this.mappedBuffer.getDouble();
		
		Record record = null;
		
		try {
			record = Record.factory(schema, vls, rank);
		} catch (TypeSchemaMismatchException | AttributeNotInSchemaException e) {
			throw new RuntimeException(e);
		}
		
		return record;
	}
	
	@Override
	public boolean delete(Record record) {
		int savedPos = this.mappedBuffer.position();
		this.mappedBuffer.position(0);
		boolean ret = false;
		while(this.mappedBuffer.position() < savedPos) {
			int currentPos = this.mappedBuffer.position();
			Record r = this.readRecord();
			if(record.equals(r)) {
				this.vacantPositions.add(currentPos);
				this.mappedBuffer.position(currentPos);
				byte[] empty = new byte[this.schemaByteSize];
				this.mappedBuffer.put(empty);
				ret = true;
				break;
			}
		}
		
		this.mappedBuffer.position(savedPos);
		return ret;
	}
	
	/**
	 * Iterator for the table
	 * @author Mgr. Radomir Skrabal
	 *
	 */
	public static class FileMappedTableIterator implements Iterator<Record>{
		
		private int localPosition = 0;
		private final FileMappedTable table;
		
		private FileMappedTableIterator(FileMappedTable table) {
			this.table = table;
		}
		
		@Override
		public boolean hasNext() {
			while(table.vacantPositions.contains(localPosition)) {
				localPosition += table.schemaByteSize;
			}
			return localPosition < this.table.mappedBuffer.position(); 
		}

		@Override
		public Record next() {			
			if(!this.hasNext()) {
				throw new NoSuchElementException("last element");
			}
			
			int savedPos = this.table.mappedBuffer.position();
			this.table.mappedBuffer.position(this.localPosition);
			Record record = this.table.readRecord();
			this.localPosition = this.table.mappedBuffer.position();
			this.table.mappedBuffer.position(savedPos);
			return record;
		}
		
	}
	
	@Override
	public Iterator<Record> iterator(){
		return new FileMappedTableIterator(this);
	}
	
	/**
	 * Gets the stream of records in this table
	 * @return
	 */
	@Override
	public Stream<Record> stream(){
		Iterator<Record> it = this.iterator();
		Stream<Record> s = null;
		if(it.hasNext()) {
			s = Stream.iterate(it.next(), r -> it.hasNext(), r -> it.next());
		}
		else {
			s = Stream.empty();
		}
		return s;
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("FileMappedTable[")
				.append(this.schema.toString())
				.append("]")
				.toString();
	}
	
	@Override
	public boolean contains(Record record) {
		return this.stream().anyMatch(r -> r.equals(record));
	}
	
	@Override
	public boolean containsNoRank(Record record) {
		return this.stream().anyMatch(r -> r.equalsNoRank(record));
	}
	
	@Override
	public boolean isEmpty() {
		return this.stream().findAny().isEmpty();
	}

	@Override
	public void close() throws IOException {
		if(this.channel != null) {
			this.channel.close();
		}
		if(this.file != null) {
			this.file.close();
		}
		if(this.tmpFile != null) {
			this.tmpFile.delete();
		}
	}

	@Override
	public Optional<Record> findNoRank(Record record) {
		return this.stream().filter(r -> record.equalsNoRank(r)).findAny();
	}
}
