package com.couchbase.lite;

/**
 * A container interface for the functions defined in the view server.
 *
 * @see <a href="http://couchdb.readthedocs.org/en/latest/query-server/javascript.html#">JavaScript query-server</a>
 */
public interface FunctionContainer {

	/**
	 * @return The next object in the current collection
	 */
	public Object getRow();

	/**
	 * @return True if this object represents a collection of values accessed by a numeric key
	 */
	public boolean isArray(final Object obj);

	/**
	 * @return An object that implements reading and writing of JSON objects
	 */
	public Object getJSON();

	/**
	 * Logs a message at the [info] level
	 *
	 * @param msg The message to log.
	 */
	public void log(final String msg);

	/**
	 * Sends a single string <em>chunk</em> in response.
	 *
	 * @param contents Text chunk.
	 */
	public void send(final String contents);

	/**
	 * Alias function for {@code JSON.stringify()}
	 *
	 * @return A JSON based representation of the object
	 */
	public String toJSON(final Object obj);
}
