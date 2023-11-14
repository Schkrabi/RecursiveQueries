package rq.common.tools;

public class Counter {
	private long value = 0;
	
	public long count(){
		return this.value;
	}
	
	public void increment() {
		this.value++;
	}
}
