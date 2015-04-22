package com.couchbase.lite;

import com.couchbase.lite.storage.SQLException;

public class CorruptedDbException extends SQLException {

    public CorruptedDbException(java.lang.String error) {
        super(error);
    }
}
