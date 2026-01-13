package rq.common.util;

/** Unique name generator */
public class NameGenerator {
	public final String globalprefix;
	private int current = 0;
	
	private NameGenerator(String prefix) {
		this.globalprefix = prefix;
	}
	
	/** Generates next unique name */
	public String next(String localPrefix) {
		var n = localPrefix + this.globalprefix + Integer.toHexString(this.current);
		this.current++;
		return n;
	}
	
	/** Generates next unique name */
	public String next() {
		return this.next("");
	}
	
	private static NameGenerator singleton = null;
	
	public static NameGenerator instance() {
		if(singleton == null) {
			singleton = new NameGenerator("");
		}
		return singleton;
	}
}
