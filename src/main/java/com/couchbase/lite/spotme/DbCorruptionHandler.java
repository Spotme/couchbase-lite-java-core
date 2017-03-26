package com.couchbase.lite.spotme;

/**
 * An interface to let apps define an action to take when database corruption is detected.
 *
 * As "net.sqlcipher.database.SQLiteDatabase" is defined in cbl/android sub-project,
 * we can't use net.sqlcipher.DatabaseErrorHandler directly, but need to have this call-back,
 * which latter will be adapted by DatabaseErrorHandler in cbl/android sub-project.
 */
public interface DbCorruptionHandler <T> {
    /**
     * The method invoked when database corruption is detected.
     * @param db the {@link SQLiteDatabase} object representing the database on which corruption
     * is detected.
     */
    void onCorruption(T db);
}
