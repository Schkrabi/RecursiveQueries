/**
 * 
 */
package rq.common.types;

/**
 * Byte serializable string of length 10
 * @author Mgr. Radomir Skrabal
 *
 */
public class Str10 extends Str implements Comparable<Str10> {
	
	private final byte ARRAY_LEN = 40;
	
	private Str10(String value, int len) {
		super(value, len);
	}
	
	public Str10() {
		super();
	}
	
	/**
	 * Creates new instance, truncating if necessary
	 * @param value value of the string
	 * @return instance
	 */
	public static Str10 factory(String value) {
		return (Str10) Str.factory(value, 10, (String val, Integer len) -> new Str10(val, len));
	}

	@Override
	public int compareTo(Str10 o) {
		return this.inner.compareTo(o.inner);
	}

	@Override
	protected int arrayLen() {
		return ARRAY_LEN;
	}
}
