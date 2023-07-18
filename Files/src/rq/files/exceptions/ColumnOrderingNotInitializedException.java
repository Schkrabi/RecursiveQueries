package rq.files.exceptions;

public class ColumnOrderingNotInitializedException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4954313276581549266L;

	public ColumnOrderingNotInitializedException() {
		super("Column orderin and schema is not initialized in TableReader.");
	}
}
