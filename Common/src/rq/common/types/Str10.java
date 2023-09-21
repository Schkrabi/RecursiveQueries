/**
 * 
 */
package rq.common.types;

import java.util.Arrays;

import rq.common.interfaces.ByteArraySerializable;

/**
 * 
 * @author Mgr. Radomir Skrabal
 *
 */
public class Str10 implements ByteArraySerializable, Comparable<Str10> {
	
	private final byte ARRAY_LEN = 32;
	
	private String inner;
	private int blen;
	
	private Str10(String value, int len) {
		this.inner = value;
		this.blen = len;
	}
	
	public Str10() {
		this.inner = "";
		this.blen = 0;
	}
	
	/**
	 * Creates new instance, truncating if necessary
	 * @param value value of the string
	 * @return instance
	 */
	public static Str10 factory(String value) {
		int len = value.getBytes().length;
		if(len <= 10) {
			return new Str10(value, len);
		}		
		return new Str10(value.substring(0, 9), len);
	}
	
	public String getInner() {
		return new String(this.inner);
	}

	@Override
	public int compareTo(Str10 o) {
		return this.inner.compareTo(o.inner);
	}

	@Override
	public byte[] toBytes() {
		byte[] btes = new byte[ARRAY_LEN];
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
		return ARRAY_LEN;
	}
	
	@Override
	public boolean equals(Object o) {
		if(! (o instanceof Str10)) {
			return false;
		}
		Str10 s = (Str10)o;
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
