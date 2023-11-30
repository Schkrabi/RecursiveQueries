package data;

import java.util.function.BiFunction;

import rq.common.table.Attribute;
import rq.common.types.Str10;

public class Toloker {

	//			2;
	//			324;
	//			2013;
	//			2118;
	//			3889;
	//			4173;
	//			5175;
	//			5406;
	//			5737;
	//			6300;
	//			6919;
	//			8707;
	//			9203;
	//			9372;
	//			10316;
	//			11027;
	
		
	//Specific similarities
	private static final String EDU_NONE = "none";
	private static final String EDU_BASIC = "basic";
	private static final String EDU_MIDDLE = "middle";
	private static final String EDU_HIGH = "high";
	public static BiFunction<Object, Object, Double> educationSimilarity = 
	(Object o1, Object o2) -> {
		Str10 s1 = (Str10)o1;
		Str10 s2 = (Str10)o2;
		switch(s1.getInner()) {
		case EDU_NONE:
			switch(s2.getInner()) {
				case EDU_NONE: return 1.0d;
				case EDU_BASIC: return 0.66d;
				case EDU_MIDDLE: return 0.33d;
				case EDU_HIGH: return 0.0d;
				default:throw new RuntimeException(s2.getInner() + " is not a valid education value");
			}
		case EDU_BASIC:
			switch(s2.getInner()){
				case EDU_NONE: return 0.66d;
				case EDU_BASIC: return 1.0d;
				case EDU_MIDDLE: return 0.66d;
				case EDU_HIGH: return 0.33d;
				default:throw new RuntimeException(s2.getInner() + " is not a valid education value");
			}	
		case EDU_MIDDLE:
			switch(s2.getInner()) {
				case EDU_NONE: return 0.33d;
				case EDU_BASIC: return 0.66d;
				case EDU_MIDDLE: return 1.0d;
				case EDU_HIGH: return 0.66d;
				default:throw new RuntimeException(s2.getInner() + " is not a valid education value");
			}
		case EDU_HIGH:
			switch(s2.getInner()) {
				case EDU_NONE: return 0.0d;
				case EDU_BASIC: return 0.33d;
				case EDU_MIDDLE: return 0.66d;
				case EDU_HIGH: return 1.0d;
				default:throw new RuntimeException(s2.getInner() + " is not a valid education value");
			}
		default: throw new RuntimeException(s1.getInner() + " is not a valid education value");
		}
	};
	//File coluns 
	public static final Attribute source = new Attribute("source", Integer.class);
	public static final Attribute target = new Attribute("target", Integer.class);
	public static final Attribute approvedRate = new Attribute("approved_rate", Double.class);
	public static final Attribute skippedRate = new Attribute("skipped_rate", Double.class);
	public static final Attribute expiredRate = new Attribute("expired_rate", Double.class);
	public static final Attribute rejectedRate = new Attribute("rejected_rate", Double.class);
	public static final Attribute education = new Attribute("education", Str10.class);
	public static final Attribute englishProfile = new Attribute("english_profile", Integer.class);
	public static final Attribute englishTested = new Attribute("english_tested", Integer.class);
	public static final Attribute banned = new Attribute("banned", Integer.class);
}
