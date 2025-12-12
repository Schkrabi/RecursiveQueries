package rq.common.util;

public interface IDeserializer<T> {
	public T deserialize(String serialized);
}
