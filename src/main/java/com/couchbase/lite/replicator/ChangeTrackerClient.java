package com.couchbase.lite.replicator;

import com.couchbase.lite.internal.InterfaceAudience;

import cz.msebera.android.httpclient.client.HttpClient;

import java.util.Map;

/**
 * @exclude
 */
@InterfaceAudience.Private
public interface ChangeTrackerClient {

    HttpClient getHttpClient();

    void changeTrackerReceivedChange(Map<String,Object> change, String lastSequence);

    void changeTrackerStopped(ChangeTracker tracker);

    void changeTrackerFinished(ChangeTracker tracker);

    void changeTrackerCaughtUp();

    void addTotalDocs(int delta);


}
