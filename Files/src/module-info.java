/**
 * 
 */
/**
 * @author r.skrabal
 *
 */
module rq.files {
	requires transitive rq.common;
	requires com.opencsv;
	exports rq.files.io;
	exports rq.files.exceptions;
	exports rq.files.helpers;
	exports rq.files.similarities;
}