package com.couchbase.lite;

import java.util.List;
import java.util.Map;

public interface FunctionCompiler {

	/**
	 * Invokes a list function and returns the resulting document
	 *
	 * @param head Information about the view.
	 * @param request The request object.
	 *
	 * @return A list of objects
	 */
	public List<Map<String, Object>> list(final Map<String, Object> head, final Map<String, Object> request);

	/**
	 * Invoke a show function
	 *
	 * @param document The contents of the document being analyzed.
	 * @param request The request object.
	 *
	 * @return The modified object
	 */
	public Object show(Map<String, Object> document, Map<String, Object> request);
}
