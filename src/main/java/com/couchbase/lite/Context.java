package com.couchbase.lite;

import java.io.File;

/**
 * Portable java wrapper around a "system specific" context.  The main implementation wraps an
 * Android context.
 *
 * The wrapper is needed so that there are no compile time dependencies on Android classes
 * within the Couchbase Lite java core library.
 *
 * This also has the nice side effect of having a single place to see exactly what parts of the
 * Android context are being used.
 */
public interface Context {

    /**
     * The files dir.  On Android implementation, simply proxies call to underlying Context
     */
    public File getFilesDir();

	/**
	 * The directory where to load shared libraries from. This may be empty.
	 */
	public String getLibraryDir(String libName);
}
