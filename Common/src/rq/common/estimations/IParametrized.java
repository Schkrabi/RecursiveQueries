package rq.common.estimations;

import java.util.Map;

public interface IParametrized {
	
	/** Get the map of all parameters*/
	public Map<String, String> params();
	
	/** String containing estimation parameters for filename*/
	public default String paramStr() {
		var sb = new StringBuilder();
		
		var etrs = this.params().entrySet();
		var i = etrs.iterator();
		while(i.hasNext()) {
			var e = i.next();
			sb.append(e.getKey())
				.append("=")
				.append(e.getValue())
				.append(i.hasNext() ? "." : "");
		}
		
		return sb.toString();
	}
}
