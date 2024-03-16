/**
 * 
 */
package rq.common.types;

import java.util.Arrays;
import java.util.function.BiFunction;

import rq.common.interfaces.ByteArraySerializable;

/**
 * 
 */
public abstract class Str implements ByteArraySerializable {
	protected String inner;
	private int blen;
	
	protected Str(String value, int len) {
		this.inner = value;
		this.blen = len;		
	}
	
	public Str() {
		this.inner = "";
		this.blen = 0;
	}
	
	protected static Str factory(String value, int maxLen, BiFunction<String, Integer, Str> constructor) {
		int len = value.getBytes().length;
		if(len <= maxLen) {
			return constructor.apply(value, len);
		}		
		return constructor.apply(new String(Arrays.copyOf(value.getBytes(), maxLen - 1)), maxLen);
	}
	
	protected abstract int arrayLen();
	
	public String getInner() {
		return new String(this.inner);
	}
	
	@Override
	public byte[] toBytes() {
		byte[] btes = new byte[this.arrayLen()];
		btes[0] = (byte)this.blen;
		byte[] sbtes = this.inner.getBytes();
		int i = 1;
		for(byte b : sbtes) {
			btes[i] = b;
			i++;
		}
		
		return btes;
	}
	
	@Override
	public void fromBytes(byte[] bytes) {
		this.blen = (int)bytes[0];		
		this.inner = new String(Arrays.copyOfRange(bytes, 1, blen + 1));
	}
	
	@Override
	public int byteArraySize() {
		return this.arrayLen();
	}
	
	@Override
	public boolean equals(Object o) {
		if(! (o instanceof Str)) {
			return false;
		}
		Str s = (Str)o;
		return this.inner.equals(s.inner)
				&& this.blen == s.blen;
	}
	
	@Override
	public int hashCode() {
		return new StringBuilder()
				.append(this.inner.hashCode())
				.append(this.blen)
				.toString()
				.hashCode();
	}
	
	@Override
	public String toString() {
		return this.inner;
	}
}
