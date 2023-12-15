package annotations;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import rq.common.algorithms.LazyRecursive;

@Retention(RUNTIME)
@Target(TYPE)
public @interface BuildsAlgorithm {
	public Class<? extends LazyRecursive> value();
}
