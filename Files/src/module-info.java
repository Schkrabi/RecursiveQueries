/**
 * 
 */
/**
 * @author r.skrabal
 *
 */
module rq.files {
	requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotations;
	requires transitive rq.common;
	requires com.opencsv;
	exports rq.files.io;
	exports rq.files.exceptions;
	exports rq.files.helpers;
	exports rq.files.contracts;
}