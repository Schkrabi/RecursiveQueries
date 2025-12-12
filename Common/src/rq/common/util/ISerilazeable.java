package rq.common.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public interface ISerilazeable<T> {
	
	public String serialize();

	public default void writeFile(String path) {
		try {
			Files.write(Path.of(path), this.serialize().getBytes());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public default void writeFile(Path path) throws IOException {
		Files.write(path, this.serialize().getBytes());
	}
}
