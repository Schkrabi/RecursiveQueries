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
	requires com.squareup.moshi;
	exports rq.files.io;
	exports rq.files.exceptions;
	exports rq.files.helpers;
	exports rq.files.similarities;
	exports rq.files.contracts;
}