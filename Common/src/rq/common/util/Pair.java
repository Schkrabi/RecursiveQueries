package rq.common.util;

public class Pair<T1, T2> {
	public final T1 first;
	public final T2 second;

	public Pair(T1 first, T2 second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof Pair<?, ?>) {
			return this.first.equals(((Pair<?, ?>) other).first) && this.second.equals(((Pair<?, ?>) other).second);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return this.first.hashCode() * this.second.hashCode();
	}

	@Override
	public String toString() {
		return "(" + first.toString() + "," + second.toString() + ")";
	}
	
	/**
	 * Creates a pair value
	 * @param <T1> Type of first element
	 * @param <T2> Type of second element
	 * @param first first element
	 * @param second second element
	 * @return pair of two elements
	 */
	public static <T1, T2> Pair<T1, T2> of(T1 first, T2 second){
		return new Pair<T1, T2>(first, second);
	}
}
