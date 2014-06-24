package com.couchbase.lite;

import com.couchbase.lite.router.URLConnection;

import java.util.List;
import java.util.Map;

public interface FunctionCompiler {

	/**
	 * Invokes a list function and returns the resulting document
	 *
	 * @param head              Information about the view.
	 * @param requestProperties A correctly formatted properties object.
	 *
	 * @return A list of objects
	 */
	public List<Map<String, Object>> list(final Map<String, Object> head, final Map<String, Object> requestProperties);

	/**
	 * Invoke a show function
	 *
	 * @param document          Contents of the document being analyzed.
	 * @param requestProperties A correctly formatted properties object.
	 *
	 * @return The modified object
	 */
	public Object show(final Map<String, Object> document, final Map<String, Object> requestProperties);

	/**
	 * Sets the request object and extracts the relevant info
	 *
	 * @param conn  The request object
	 */
	public void setRequestObject(final URLConnection conn);

	/**
	 * @return  A formatted object with the necessary request properties.
	 */
	public Map<String, Object> getRequestProperties();
}
