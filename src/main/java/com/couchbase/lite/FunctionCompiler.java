package com.couchbase.lite;

import com.couchbase.lite.router.URLConnection;

import java.util.Map;

public interface FunctionCompiler {

	/**
	 * @return  A formatted object with the necessary request properties.
	 */
	public Map<String, Object> getRequestProperties();

	/**
	 * Invokes a list function and returns the resulting document
	 *
	 * @param listName          Name of the list function to execute
	 * @param head              Information about the view.
	 *
	 * @return A JSON representation of the response
	 */
	public String list(final String listName, final Map<String, Object> head) throws CouchbaseLiteException;

	/**
	 * @return A new instance of this compiler
	 */
	public FunctionCompiler newInstance();

	/**
	 * Set the design document that this compiler will work with
	 *
	 * @param document  Design document
	 */
	public void setDesignDocument(final Map<String, Object> document);

	/**
	 * Sets the request object and extracts the relevant info
	 *
	 * @param conn  The request object
	 */
	public void setRequestObject(final URLConnection conn);

	/**
	 * @param result The result of the view where this list function was invoked
	 */
	public void setViewResult(final Map<String, Object> result);

	/**
	 * Invoke a show function
	 *
	 * @param showName          Name of the show function to execute
	 * @param document          Contents of the document being analyzed.
	 *
	 * @return A JSON response of the response
	 */
	public String show(final String showName, final Map<String, Object> document) throws CouchbaseLiteException;
}
