package rq.files.similarities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import rq.common.table.Attribute;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.helpers.ValueParser;

public class NominalSimilarity {

	public NominalSimilarity() {
		// TODO Auto-generated constructor stub
	}
	
	private static Object[] parseHeader(Attribute a, Stream<String> vls) {
		return vls.map(s -> {
			try {
				return ValueParser.parse(a, s.trim());
			} catch (ClassNotInContextException e) {
				throw new RuntimeException(e);
			}
		}).toArray();
	}
	
	/** Parses the sting with .csv similarity */
	private static Map<Object, Map<Object, Double>> parse(String data) throws ClassNotFoundException, ClassNotInContextException{
		var ret = new LinkedHashMap<Object, Map<Object, Double>>();
		Attribute a = null;
		Object[] hdrLine = null;
		
		for(var line : data.split("\n")) {
			var vls = line.split(",");
			
			if(a == null) {
				a = Attribute.parse(vls[0]);
				hdrLine = parseHeader(a, Stream.of(vls).skip(1));
				continue;
			}			
			
			var m = new LinkedHashMap<Object, Double>();
			for(int i = 1; i < vls.length; i++) {
				var val = Double.parseDouble(vls[i]);
				m.put(hdrLine[i - 1], val);
			}
			var vl = ValueParser.parse(a, vls[0].trim());
			ret.put(vl, m);
		}
		return ret;
	}
	
	/** Loads similarity from a file */
	public static BiFunction<Object, Object, Double> readFromFile(Path path) throws IOException, ClassNotFoundException, ClassNotInContextException{
		var data = Files.readString(path);
		var map = parse(data);
		
		return (x, y) -> map.get(x).get(y);
	}
}
