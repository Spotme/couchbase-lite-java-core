package com.couchbase.lite.replicator;

import com.couchbase.lite.internal.InterfaceAudience;

import java.util.Map;

import cz.msebera.android.httpclient.client.HttpClient;

/**
 * @exclude
 */
@InterfaceAudience.Private
public interface ChangeTrackerClient {

    HttpClient getHttpClient();

    void changeTrackerReceivedChange(Map<String,Object> change);

    void changeTrackerStopped(ChangeTracker tracker);

    void changeTrackerFinished(ChangeTracker tracker);

    void changeTrackerCaughtUp();


}
