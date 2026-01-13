package rq.common.similarities;

import java.util.function.BiFunction;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import rq.common.estimations.SignatureProvider;

@JsonTypeInfo(
		use = JsonTypeInfo.Id.NAME,
		include = JsonTypeInfo.As.PROPERTY,
		property = "type")
@JsonSubTypes({
		@JsonSubTypes.Type(value = LinearNumSimilarity.class, name = "linearNum"),
		@JsonSubTypes.Type(value = NominalSimilarity.class, name = "nominal"),
		@JsonSubTypes.Type(value = DateTimeSimilarity.class, name = "datetime"),
		@JsonSubTypes.Type(value = LinearIntSimilarity.class, name = "linearInt")})
public interface ISimilarity<T> extends BiFunction<T, T, Double>, SignatureProvider {

}
