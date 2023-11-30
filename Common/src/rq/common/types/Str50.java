package rq.common.types;

public class Str50 extends Str implements Comparable<Str50> {

	private final long ARRAY_LEN = 160;
	
	private Str50(String value, int len) {
		super(value, len);
	}
	
	public Str50() {
		super();
	}
	
	/**
	 * Creates new instance, truncating if necessary
	 * @param value value of the string
	 * @return instance
	 */
	public static Str50 factory(String value) {
		return (Str50)Str.factory(value, 50, (String val, Integer len) -> new Str50(val, len));
	}
	
	@Override
	public int compareTo(Str50 o) {
		return this.inner.compareTo(o.inner);
	}

	@Override
	protected int arrayLen() {
		return (int) ARRAY_LEN;
	}
}
