package com.couchbase.lite;

import java.io.File;

/**
 * Option flags for Manager initialization.
 */
public class ManagerOptions {

    /**
     * Location of the database files
     */
    private File databaseDir;

    /**
     *  No modifications to databases are allowed.
     */
    private boolean readOnly;

    public ManagerOptions() {
    }

    public File getDatabaseDir() {
        return databaseDir;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setDatabaseDir(final File dir) {
        databaseDir = dir;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

}
