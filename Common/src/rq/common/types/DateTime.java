/**
 * 
 */
package rq.common.types;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.time.LocalDateTime;

import rq.common.interfaces.ByteArraySerializable;

/**
 * Byte Array Serializable Date Time
 * @author Mgr. Radomir Skrabal
 *
 */
public class DateTime implements ByteArraySerializable, Comparable<DateTime> {
	
	private LocalDateTime inner;
	
	public DateTime(LocalDateTime dateTime) {
		this.inner = dateTime;
	}
	
	public DateTime() {
		this.inner = LocalDateTime.MIN;
	}
	
	public LocalDateTime getInner() {
		return this.inner;
	}

	@Override
	public byte[] toBytes() {
		ByteBuffer buff = MappedByteBuffer.allocate(this.byteArraySize());
		buff.putInt(this.inner.getYear());
		buff.putInt(this.inner.getMonthValue());
		buff.putInt(this.inner.getDayOfMonth());
		buff.putInt(this.inner.getHour());
		buff.putInt(this.inner.getMinute());
		buff.putInt(this.inner.getSecond());
		buff.putInt(this.inner.getNano());
		
		return buff.array();
	}

	@Override
	public void fromBytes(byte[] bytes) {
		ByteBuffer buff = MappedByteBuffer.wrap(bytes);
		
		int year = buff.getInt();
		int month = buff.getInt();
		int day = buff.getInt();
		int hour = buff.getInt();
		int minute = buff.getInt();
		int second = buff.getInt();
		int nano = buff.getInt();
		
		this.inner = LocalDateTime.of(
				year, 
				month, 
				day, 
				hour, 
				minute, 
				second,
				nano);
	}

	@Override
	public int byteArraySize() {
		return 7 * Integer.BYTES;
	}
	
	@Override
	public String toString() {
		return this.inner.toString();
	}
	
	@Override 
	public int hashCode() {
		return this.inner.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof DateTime) {
			DateTime other = (DateTime)o;
			return this.inner.equals(other.inner);
		}
		return false;
	}

	@Override
	public int compareTo(DateTime o) {
		return this.inner.compareTo(o.inner);
	}

}
