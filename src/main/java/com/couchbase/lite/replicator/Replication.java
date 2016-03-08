package com.couchbase.lite.replicator;

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Misc;
import com.couchbase.lite.RevisionList;
import com.couchbase.lite.Status;
import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.auth.AuthenticatorImpl;
import com.couchbase.lite.auth.FacebookAuthorizer;
import com.couchbase.lite.auth.PersonaAuthorizer;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.support.BatchProcessor;
import com.couchbase.lite.support.Batcher;
import com.couchbase.lite.support.CouchbaseLiteHttpClientFactory;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.PersistentCookieStore;
import com.couchbase.lite.support.RemoteMultipartDownloaderRequest;
import com.couchbase.lite.support.RemoteMultipartRequest;
import com.couchbase.lite.support.RemoteRequest;
import com.couchbase.lite.support.RemoteRequestCompletionBlock;
import com.couchbase.lite.util.CollectionUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.TextUtils;
import com.couchbase.lite.util.URIUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import cz.msebera.android.httpclient.Header;
import cz.msebera.android.httpclient.HttpResponse;
import cz.msebera.android.httpclient.client.HttpResponseException;
import cz.msebera.android.httpclient.cookie.Cookie;
import org.apache.http.entity.mime.MultipartEntity;
import cz.msebera.android.httpclient.impl.cookie.BasicClientCookie2;

/**
 * A Couchbase Lite pull or push Replication between a local and a remote Database.
 */
public abstract class Replication {

    private static int lastSessionID = 0;

	protected boolean continuous;
    protected String filterName;
    protected ScheduledExecutorService workExecutor;
    protected Database db;
    protected URL remote;

	protected final String remoteDbUuid;
    protected String lastSequence;
    protected boolean running;
    protected boolean active;
    protected Throwable error;
    protected String sessionID;
    protected Batcher<RevisionInternal> batcher;
    protected int asyncTaskCount;
    protected AtomicInteger completedChangesCount;
    private AtomicInteger changesCount;
    protected HttpClientFactory clientFactory;
    private final List<ChangeListener> changeListeners;
    protected List<String> documentIDs;

    protected Map<String, Object> filterParams;
    protected ExecutorService remoteRequestExecutor;
    protected Authenticator authenticator;
    private ReplicationStatus status = ReplicationStatus.REPLICATION_STOPPED;
    protected Map<String, Object> requestHeaders;
    private int revisionsFailed;
	/**
	 * To re-start continuous replication
	 */
    private ScheduledFuture retryIfReadyFuture;
    private final Map<RemoteRequest, Future> requests;
    private String serverType;
    private String remoteCheckpointDocID;
	private String replicationID;

    private CollectionUtils.Functor<Map<String,Object>,Map<String,Object>> propertiesTransformationBlock;

    protected CollectionUtils.Functor<RevisionInternal,RevisionInternal> revisionBodyTransformationBlock;

    // Batch size
    protected int batchSize = 200;

    protected static int RETRY_DELAY = 60;
    protected static final int PROCESSOR_DELAY = 500;

	/** We should have the same number of http connection with "keep-alive" **/
	public static final int EXECUTOR_THREAD_POOL_SIZE = 5;

    public static final int DEFAULT_BATCH_SIZE = 100;

    /**
     * @exclude
     */
    public static final String BY_CHANNEL_FILTER_NAME = "sync_gateway/bychannel";

    /**
     * @exclude
     */
    public static final String CHANNELS_QUERY_PARAM = "channels";

    /**
     * @exclude
     */
    public static final String REPLICATOR_DATABASE_NAME = "_replicator";

    /**
     * Options for what metadata to include in document bodies
     */
    public enum ReplicationStatus {
        /** The replication is finished or hit a fatal error. */
        REPLICATION_STOPPED,
        /** The remote host is currently unreachable. */
        REPLICATION_OFFLINE,
        /** Continuous replication is caught up and waiting for more changes.*/
        REPLICATION_IDLE,
        /** The replication is actively transferring data. */
        REPLICATION_ACTIVE
    }

    /**
     * Encodes the given document id for use in an URI.
     * <p>
     * Avoids encoding the slash in _design documents since it may cause a 301 redirect.
     */
    /* package */
    String encodeDocumentId(String docId) {
        if (docId.startsWith("_design/")) {
            // http://docs.couchdb.org/en/1.6.1/http-api.html#cap-/{db}/_design/{ddoc}
            String designDocId = docId.substring("_design/".length());
            return "_design/".concat(URIUtils.encode(designDocId));
        } else {
            return URIUtils.encode(docId);
        }
    }

    /**
     * Private Constructor
     * @exclude
     */
    @InterfaceAudience.Private
    /* package */ Replication(Database db, URL remote, String replID, String remoteDbUuid, boolean continuous, Integer sizeBatch, ScheduledExecutorService workExecutor) {
        this(db, remote, replID, remoteDbUuid, continuous, sizeBatch, null, workExecutor);
    }

    /**
     * Private Constructor
     * @exclude
     */
    @InterfaceAudience.Private
    /* package */ Replication(Database db, URL remote, String replID, String remoteDbUuid, boolean continuous, Integer sizeBatch, HttpClientFactory clientFactory, ScheduledExecutorService workExecutor) {

        this.db = db;
	    this.replicationID = replID;
        this.continuous = continuous;
        if (sizeBatch != null) this.batchSize = sizeBatch;
        else this.batchSize = DEFAULT_BATCH_SIZE;
        this.workExecutor = workExecutor;
        this.remote = remote;
	    this.remoteDbUuid = remoteDbUuid;
        this.remoteRequestExecutor = Executors.newFixedThreadPool(EXECUTOR_THREAD_POOL_SIZE);
        this.changeListeners = new CopyOnWriteArrayList<ChangeListener>();
        this.requestHeaders = new HashMap<String, Object>();
        this.requests = new ConcurrentHashMap<RemoteRequest, Future>();

        this.completedChangesCount = new AtomicInteger(0);
        this.changesCount = new AtomicInteger(0);

        if (remote.getQuery() != null && !remote.getQuery().isEmpty()) {

            URI uri = URI.create(remote.toExternalForm());

            String personaAssertion = URIUtils.getQueryParameter(uri, PersonaAuthorizer.QUERY_PARAMETER);
            if (personaAssertion != null && !personaAssertion.isEmpty()) {
                String email = PersonaAuthorizer.registerAssertion(personaAssertion);
                PersonaAuthorizer authorizer = new PersonaAuthorizer(email);
                setAuthenticator(authorizer);
            }

            String facebookAccessToken = URIUtils.getQueryParameter(uri, FacebookAuthorizer.QUERY_PARAMETER);
            if (facebookAccessToken != null && !facebookAccessToken.isEmpty()) {
                String email = URIUtils.getQueryParameter(uri, FacebookAuthorizer.QUERY_PARAMETER_EMAIL);
                FacebookAuthorizer authorizer = new FacebookAuthorizer(email);
                URL remoteWithQueryRemoved = null;
                try {
                    remoteWithQueryRemoved = new URL(remote.getProtocol(), remote.getHost(), remote.getPort(), remote.getPath());
                } catch (MalformedURLException e) {
                    throw new IllegalArgumentException(e);
                }
                authorizer.registerAccessToken(facebookAccessToken, email, remoteWithQueryRemoved.toExternalForm());
                setAuthenticator(authorizer);
            }

            // we need to remove the query from the URL, since it will cause problems when
            // communicating with sync gw / couchdb
            try {
                this.remote = new URL(remote.getProtocol(), remote.getHost(), remote.getPort(), remote.getPath());
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException(e);
            }

        }

        batcher = new Batcher<RevisionInternal>(workExecutor, batchSize, PROCESSOR_DELAY, new BatchProcessor<RevisionInternal>() {
            @Override
            public void process(List<RevisionInternal> inbox) {

                try {
                    Log.v(Log.TAG_SYNC, "*** %s: BEGIN processInbox (%d sequences)", this, inbox.size());
                    processInbox(new RevisionList(inbox));
                    Log.v(Log.TAG_SYNC, "*** %s: END processInbox (lastSequence=%s)", this, lastSequence);
                    Log.v(Log.TAG_SYNC, "%s: batcher calling updateActive()", this);
                    updateActive();
                } catch (Exception e) {
                   Log.e(Log.TAG_SYNC,"ERROR: processInbox failed: ",e);
                    throw new RuntimeException(e);
                }
            }
        });

        setClientFactory(clientFactory);

    }

    /**
     * Set the HTTP client factory if one was passed in, or use the default
     * set in the manager if available.
     * @param clientFactory
     */
    @InterfaceAudience.Private
    protected void setClientFactory(HttpClientFactory clientFactory) {
        Manager manager = null;
        if (this.db != null) {
            manager = this.db.getManager();
        }
        HttpClientFactory managerClientFactory = null;
        if (manager != null) {
            managerClientFactory = manager.getDefaultHttpClientFactory();
        }
        if (clientFactory != null) {
            this.clientFactory = clientFactory;
        } else {
            if (managerClientFactory != null) {
                this.clientFactory = managerClientFactory;
            } else {
                PersistentCookieStore cookieStore = db.getPersistentCookieStore();
                this.clientFactory = new CouchbaseLiteHttpClientFactory(cookieStore);
            }
        }
    }


    /**
     * Get the local database which is the source or target of this replication
     */
    @InterfaceAudience.Public
    public Database getLocalDatabase() {
        return db;
    }

    /**
     * Get the remote URL which is the source or target of this replication
     */
    @InterfaceAudience.Public
    public URL getRemoteUrl() {
        return remote;
    }

	/**
	 * Get the uuid of remote db if provided by server side. Null otherwise.
	 */
	public String getRemoteDbUuid() {
		return remoteDbUuid;
	}

    /**
     * Is this a pull replication?  (Eg, it pulls data from Sync Gateway -> Device running CBL?)
     */
    @InterfaceAudience.Public
    public abstract boolean isPull();


    /**
     * Should the target database be created if it doesn't already exist? (Defaults to NO).
     */
    @InterfaceAudience.Public
    public abstract boolean shouldCreateTarget();

    /**
     * Set whether the target database be created if it doesn't already exist?
     */
    @InterfaceAudience.Public
    public abstract void setCreateTarget(boolean createTarget);

    /**
     * Should the replication operate continuously, copying changes as soon as the
     * source database is modified? (Defaults to NO).
     */
    @InterfaceAudience.Public
    public boolean isContinuous() {
        return continuous;
    }

    /**
     * Set whether the replication should operate continuously.
     */
    @InterfaceAudience.Public
    public void setContinuous(boolean continuous) {
        if (!isRunning()) {
            this.continuous = continuous;
        }
    }

    /**
     * Name of an optional filter function to run on the source server. Only documents for
     * which the function returns true are replicated.
     *
     * For a pull replication, the name looks like "designdocname/filtername".
     * For a push replication, use the name under which you registered the filter with the Database.
     */
    @InterfaceAudience.Public
    public String getFilter() {
        return filterName;
    }

    /**
     * Set the filter to be used by this replication
     */
    @InterfaceAudience.Public
    public void setFilter(String filterName) {
        this.filterName = filterName;
    }

    /**
     * Parameters to pass to the filter function.  Should map strings to strings.
     */
    @InterfaceAudience.Public
    public Map<String, Object> getFilterParams() {
        return filterParams;
    }

    /**
     * Set parameters to pass to the filter function.
     */
    @InterfaceAudience.Public
    public void setFilterParams(Map<String, Object> filterParams) {
        this.filterParams = filterParams;
    }

    /**
     * List of Sync Gateway channel names to filter by; a nil value means no filtering, i.e. all
     * available channels will be synced.  Only valid for pull replications whose source database
     * is on a Couchbase Sync Gateway server.  (This is a convenience that just reads or
     * changes the values of .filter and .query_params.)
     */
    @InterfaceAudience.Public
    public List<String> getChannels() {
        if (filterParams == null || filterParams.isEmpty()) {
            return new ArrayList<String>();
        }
        String params = (String) filterParams.get(CHANNELS_QUERY_PARAM);
        if (!isPull() || getFilter() == null || !getFilter().equals(BY_CHANNEL_FILTER_NAME) || params == null || params.isEmpty()) {
            return new ArrayList<String>();
        }
        String[] paramsArray = params.split(",");
        return new ArrayList<String>(Arrays.asList(paramsArray));
    }

    /**
     * Set the list of Sync Gateway channel names
     */
    @InterfaceAudience.Public
    public void setChannels(List<String> channels) {
        if (channels != null && !channels.isEmpty()) {
            if (!isPull()) {
                Log.w(Log.TAG_SYNC, "filterChannels can only be set in pull replications");
                return;
            }
            setFilter(BY_CHANNEL_FILTER_NAME);
            Map<String, Object> filterParams = new HashMap<String, Object>();
            filterParams.put(CHANNELS_QUERY_PARAM, TextUtils.join(",", channels));
            setFilterParams(filterParams);
        } else if (getFilter().equals(BY_CHANNEL_FILTER_NAME)) {
            setFilter(null);
            setFilterParams(null);
        }
    }

    /**
     * Extra HTTP headers to send in all requests to the remote server.
     * Should map strings (header names) to strings.
     */
    @InterfaceAudience.Public
    public Map<String, Object> getHeaders() {
        return requestHeaders;
    }

    /**
     * Set Extra HTTP headers to be sent in all requests to the remote server.
     */
    @InterfaceAudience.Public
    public void setHeaders(Map<String, Object> requestHeadersParam) {
        if (requestHeadersParam != null && !requestHeaders.equals(requestHeadersParam)) {
            requestHeaders = requestHeadersParam;
        }
    }

    /**
     * Gets the documents to specify as part of the replication.
     */
    @InterfaceAudience.Public
    public List<String> getDocIds() {
        return documentIDs;
    }

    /**
     * Sets the documents to specify as part of the replication.
     */
    @InterfaceAudience.Public
    public void setDocIds(List<String> docIds) {
        documentIDs = docIds;
    }

    /**
     * The replication's current state, one of {stopped, offline, idle, active}.
     */
    @InterfaceAudience.Public
    public ReplicationStatus getStatus() {
        return status;
    }

    /**
     * The number of completed changes processed, if the task is active, else 0 (observable).
     */
    @InterfaceAudience.Public
    public int getCompletedChangesCount() {
        return completedChangesCount.get();
    }

    /**
     * The total number of changes to be processed, if the task is active, else 0 (observable).
     */
    @InterfaceAudience.Public
    public int getChangesCount() {
        return changesCount.get();
    }

    /**
     * True while the replication is running, False if it's stopped.
     * Note that a continuous replication never actually stops; it only goes idle waiting for new
     * data to appear.
     */
    @InterfaceAudience.Public
    public boolean isRunning() {
        return running;
    }

    /**
     * The error status of the replication, or null if there have not been any errors since
     * it started.
     */
    @InterfaceAudience.Public
    public Throwable getLastError() {
        return error;
    }

    /**
     * Starts the replication, asynchronously.
     */
    @InterfaceAudience.Public
    public void start() {

        if (!db.isOpen()) { // Race condition: db closed before replication starts
            Log.w(Log.TAG_SYNC, "Not starting replication because db.isOpen() returned false.");
            return;
        }

        if (running) {
            return;
        }

        db.addReplication(this);
        db.addActiveReplication(this);

        final CollectionUtils.Functor<Map<String,Object>,Map<String,Object>> xformer = propertiesTransformationBlock;
        if (xformer != null) {
            revisionBodyTransformationBlock = new CollectionUtils.Functor<RevisionInternal, RevisionInternal>() {
                @Override
                public RevisionInternal invoke(RevisionInternal rev) {
                    Map<String,Object> properties = rev.getProperties();
                    Map<String, Object> xformedProperties = xformer.invoke(properties);
                    if (xformedProperties == null) {
                        rev = null;
                    } else if (xformedProperties != properties) {
                        assert(xformedProperties != null);
                        assert(xformedProperties.get("_id").equals(properties.get("_id")));
                        assert(xformedProperties.get("_rev").equals(properties.get("_rev")));
                        RevisionInternal nuRev = new RevisionInternal(rev.getProperties(), db);
                        nuRev.setProperties(xformedProperties);
                        rev = nuRev;
                    }
                    return rev;
                }
            };
        }


        this.sessionID = String.format("repl%03d", ++lastSessionID);
        Log.v(Log.TAG_SYNC, "%s: STARTING ...", this);
        running = true;
        lastSequence = null;

        checkSession();

    }

    /**
     * Stops replication, asynchronously.
     */
    @InterfaceAudience.Public
    public void stop() {
        if (!running) {
            return;
        }
        Log.v(Log.TAG_SYNC, "%s: STOPPING...", this);
        if (batcher != null) {
            batcher.clear();  // no sense processing any pending changes
        } else {
            Log.v(Log.TAG_SYNC, "%s: stop() called, not calling batcher.clear() since it's null", this);
        }
        continuous = false;
        stopRemoteRequests();
        cancelPendingRetryIfReady();
        if (db != null) {
            db.forgetReplication(this);
        } else {
            Log.v(Log.TAG_SYNC, "%s: stop() called, not calling db.forgetReplication() since it's null", this);
        }
        Log.v(Log.TAG_SYNC, "%s: calling stopped()", this);
        stopped();
}

    /**
     * Restarts a completed or failed replication.
     */
    @InterfaceAudience.Public
    public void restart() {
        // TODO: add the "started" flag and check it here
        stop();
        start();
    }

    /**
     * Adds a change delegate that will be called whenever the Replication changes.
     */
    @InterfaceAudience.Public
    public void addChangeListener(ChangeListener changeListener) {
        changeListeners.add(changeListener);
    }

    /**
     * Return a string representation of this replication.
     *
     * The credentials will be masked in order to avoid passwords leaking into logs.
     */
    @Override
    @InterfaceAudience.Public
    public String toString() {
        String maskedRemoteWithoutCredentials = (remote != null ? remote.toExternalForm() : "");
        maskedRemoteWithoutCredentials = maskedRemoteWithoutCredentials.replaceAll("://.*:.*@", "://---:---@");
        String name = getClass().getSimpleName() + "@" + Integer.toHexString(hashCode()) + "[" + maskedRemoteWithoutCredentials + "]";
        return name;
    }


    /**
     * Sets an HTTP cookie for the Replication.
     *
     * @param name The name of the cookie.
     * @param value The value of the cookie.
     * @param path The path attribute of the cookie.  If null or empty, will use remote.getPath()
     * @param maxAge The maxAge, in milliseconds, that this cookie should be valid for.
     * @param secure Whether the cookie should only be sent using a secure protocol (e.g. HTTPS).
     * @param httpOnly (ignored) Whether the cookie should only be used when transmitting HTTP, or HTTPS, requests thus restricting access from other, non-HTTP APIs.
     */
    @InterfaceAudience.Public
    public void setCookie(String name, String value, String path, long maxAge, boolean secure, boolean httpOnly) {
        Date now = new Date();
        Date expirationDate = new Date(now.getTime() + maxAge);
        setCookie(name, value, path, expirationDate, secure, httpOnly);
    }

    /**
     * Sets an HTTP cookie for the Replication.
     *
     * @param name The name of the cookie.
     * @param value The value of the cookie.
     * @param path The path attribute of the cookie.  If null or empty, will use remote.getPath()
     * @param expirationDate The expiration date of the cookie.
     * @param secure Whether the cookie should only be sent using a secure protocol (e.g. HTTPS).
     * @param httpOnly (ignored) Whether the cookie should only be used when transmitting HTTP, or HTTPS, requests thus restricting access from other, non-HTTP APIs.
     */
    @InterfaceAudience.Public
    public void setCookie(String name, String value, String path, Date expirationDate, boolean secure, boolean httpOnly) {
        if (remote == null) {
            throw new IllegalStateException("Cannot setCookie since remote == null");
        }
        BasicClientCookie2 cookie = new BasicClientCookie2(name, value);
        cookie.setDomain(remote.getHost());
        if (path != null && path.length() > 0) {
            cookie.setPath(path);
        } else {
            cookie.setPath(remote.getPath());
        }

        cookie.setExpiryDate(expirationDate);
        cookie.setSecure(secure);
        List<Cookie> cookies = Arrays.asList((Cookie)cookie);
        this.clientFactory.addCookies(cookies);

    }

    /**
     * Deletes an HTTP cookie for the Replication.
     *
     * @param name The name of the cookie.
     */
    @InterfaceAudience.Public
    public void deleteCookie(String name) {
        this.clientFactory.deleteCookie(name);
    }


    /**
     * The type of event raised by a Replication when any of the following
     * properties change: mode, running, error, completed, total.
     */
    @InterfaceAudience.Public
    public static class ChangeEvent {

        private Replication source;

        public ChangeEvent(Replication source) {
            this.source = source;
        }

        public Replication getSource() {
            return source;
        }

    }

    /**
     * A delegate that can be used to listen for Replication changes.
     */
    @InterfaceAudience.Public
    public static interface ChangeListener {
        public void changed(ChangeEvent event);
    }

    /**
     * Removes the specified delegate as a listener for the Replication change event.
     */
    @InterfaceAudience.Public
    public void removeChangeListener(ChangeListener changeListener) {
        changeListeners.remove(changeListener);
    }

    /**
     * Set the Authenticator used for authenticating with the Sync Gateway
     */
    @InterfaceAudience.Public
    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    /**
     * Get the Authenticator used for authenticating with the Sync Gateway
     */
    @InterfaceAudience.Public
    public Authenticator getAuthenticator() {
        return authenticator;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void databaseClosing() {
        saveLastSequence();
        stop();
        clearDbRef();
    }

    /**
     * If we're in the middle of saving the checkpoint and waiting for a response, by the time the
     * response arrives _db will be nil, so there won't be any way to save the checkpoint locally.
     * To avoid that, pre-emptively save the local checkpoint now.
     *
     * @exclude
     */
    private void clearDbRef() {
        if (lastSequence != null && db != null) {
            db.setLastSequence(lastSequence, remoteCheckpointDocID(), !isPull());
            db = null;
        } else {
            Log.v(Log.TAG_SYNC, "%s: clearDbRef() not doing anything.  lastSequence: %s db: %s", this, lastSequence, db);
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String getLastSequence() {
        return lastSequence;
    }

    protected ScheduledFuture saveLastSequenceFuture;

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void setLastSequence(String lastSequenceIn) {
        if (lastSequenceIn != null && !lastSequenceIn.equals(lastSequence)) {
            Log.v(Log.TAG_SYNC, "%s: Setting lastSequence to %s from(%s)", this, lastSequenceIn, lastSequence );
            lastSequence = lastSequenceIn;

            if (saveLastSequenceFuture != null && (!saveLastSequenceFuture.isDone() || !saveLastSequenceFuture.isCancelled())) {
                saveLastSequenceFuture.cancel(false);
            }

            saveLastSequenceFuture = workExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                    saveLastSequence();
                }
            }, 2 * 1000, TimeUnit.MILLISECONDS);
        }
    }

    @InterfaceAudience.Private
    /* package */ void addToCompletedChangesCount(int delta) {
        int previousVal = this.completedChangesCount.getAndAdd(delta);
        Log.v(Log.TAG_SYNC, "%s: Incrementing completedChangesCount count from %s by adding %d -> %d", this, previousVal, delta, completedChangesCount.get());
        notifyChangeListeners();
    }

    @InterfaceAudience.Private
    /* package */ void addToChangesCount(int delta) {
        int previousVal = this.changesCount.getAndAdd(delta);
        if (changesCount.get() < 0) {
            Log.w(Log.TAG_SYNC, "Changes count is negative, this could indicate an error");
        }
        Log.v(Log.TAG_SYNC, "%s: Incrementing changesCount count from %s by adding %d -> %d", this, previousVal, delta, changesCount.get());
        notifyChangeListeners();
    }


    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String getSessionID() {
        return sessionID;
    }

	/**
	 * @exclude
	 */
	@InterfaceAudience.Private
	public String getReplicationID() {
		return replicationID;
	}

    @InterfaceAudience.Private
    protected void checkSession() {
        // REVIEW : This is not in line with the iOS implementation
        if (getAuthenticator() != null && ((AuthenticatorImpl)getAuthenticator()).usesCookieBasedLogin()) {
            checkSessionAtPath("/_session");
        } else {
            fetchRemoteCheckpointDoc();
        }
    }

    @InterfaceAudience.Private
    protected void checkSessionAtPath(final String sessionPath) {

        Log.v(Log.TAG_SYNC, "%s | %s: checkSessionAtPath() calling asyncTaskStarted()", this, Thread.currentThread());

        asyncTaskStarted();
        sendAsyncRequest("GET", sessionPath, null, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(Object result, Throwable error) {

                try {
                    if (error != null) {
                        // If not at /db/_session, try CouchDB location /_session
                        if (error instanceof HttpResponseException &&
                                ((HttpResponseException) error).getStatusCode() == 404 &&
                                sessionPath.equalsIgnoreCase("/_session")) {

                            checkSessionAtPath("_session");
                            return;
                        }
                        Log.e(Log.TAG_SYNC, this + ": Session check failed", error);
                        setError(error);

                    } else {
                        Map<String, Object> response = (Map<String, Object>) result;
                        Map<String, Object> userCtx = (Map<String, Object>) response.get("userCtx");
                        String username = (String) userCtx.get("name");
                        if (username != null && username.length() > 0) {
                            Log.d(Log.TAG_SYNC, "%s Active session, logged in as %s", this, username);
                            fetchRemoteCheckpointDoc();
                        } else {
                            Log.d(Log.TAG_SYNC, "%s No active session, going to login", this);
                            login();
                        }
                    }

                } finally {
                    Log.v(Log.TAG_SYNC, "%s | %s: checkSessionAtPath() calling asyncTaskFinished()", this, Thread.currentThread());

                    asyncTaskFinished(1);
                }
            }

        });
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public abstract void beginReplicating();

    @InterfaceAudience.Private
    protected void stopped() {
        Log.v(Log.TAG_SYNC, "%s: STOPPED", this);
        running = false;

        notifyChangeListeners();

        saveLastSequence();

        batcher = null;

        clearDbRef();  // db no longer tracks me so it won't notify me when it closes; clear ref now

    }

    @InterfaceAudience.Private
    private void notifyChangeListeners() {
        updateProgress();
        for (ChangeListener listener : changeListeners) {
            ChangeEvent changeEvent = new ChangeEvent(this);
            listener.changed(changeEvent);
        }

    }

    @InterfaceAudience.Private
    protected void login() {
        Map<String, String> loginParameters = ((AuthenticatorImpl)getAuthenticator()).loginParametersForSite(remote);
        if (loginParameters == null) {
            Log.d(Log.TAG_SYNC, "%s: %s has no login parameters, so skipping login", this, getAuthenticator());
            fetchRemoteCheckpointDoc();
            return;
        }

        final String loginPath = ((AuthenticatorImpl)getAuthenticator()).loginPathForSite(remote);

        Log.d(Log.TAG_SYNC, "%s: Doing login with %s at %s", this, getAuthenticator().getClass(), loginPath);

        Log.v(Log.TAG_SYNC, "%s | %s: login() calling asyncTaskStarted()", this, Thread.currentThread());

        asyncTaskStarted();
        sendAsyncRequest("POST", loginPath, loginParameters, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(Object result, Throwable e) {
                try {
                    if (e != null) {
                        Log.d(Log.TAG_SYNC, "%s: Login failed for path: %s", this, loginPath);
                        setError(e);
                    }
                    else {
                        Log.v(Log.TAG_SYNC, "%s: Successfully logged in!", this);
                        fetchRemoteCheckpointDoc();
                    }
                } finally {
                    Log.v(Log.TAG_SYNC, "%s | %s: login() calling asyncTaskFinished()", this, Thread.currentThread());

                    asyncTaskFinished(1);
                }
            }

        });

    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public synchronized void asyncTaskStarted() {
        Log.v(Log.TAG_SYNC, "%s: asyncTaskStarted %d -> %d", this, this.asyncTaskCount, this.asyncTaskCount + 1);
        if (asyncTaskCount++ == 0) {
            Log.v(Log.TAG_SYNC, "%s: asyncTaskStarted() calling updateActive()", this);
            updateActive();
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public synchronized void asyncTaskFinished(int numTasks) {
        Log.v(Log.TAG_SYNC, "%s: asyncTaskFinished %d -> %d", this, this.asyncTaskCount, this.asyncTaskCount - numTasks);
        this.asyncTaskCount -= numTasks;
        assert(asyncTaskCount >= 0);
        if (asyncTaskCount == 0) {
            Log.v(Log.TAG_SYNC, "%s: asyncTaskFinished() calling updateActive()", this);
            updateActive();
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void updateActive() {
        try {
            int batcherCount = 0;
            if (batcher != null) {
                batcherCount = batcher.count();
            } else {
                Log.w(Log.TAG_SYNC, "%s: batcher object is null.", this);
            }
            boolean newActive = batcherCount > 0 || asyncTaskCount > 0;
            Log.d(Log.TAG_SYNC, "%s: updateActive() called.  active: %s, newActive: %s batcherCount: %d, asyncTaskCount: %d", this, active, newActive, batcherCount, asyncTaskCount); 
            if (active != newActive) {
                Log.d(Log.TAG_SYNC, "%s: Progress: set active = %s asyncTaskCount: %d batcherCount: %d", this, newActive, asyncTaskCount, batcherCount);
                active = newActive;
                Log.d(Log.TAG_SYNC, "%s: Progress: active = %s", this, active);
                notifyChangeListeners();

                Log.d(Log.TAG_SYNC, "%s: Progress: active = %s ..", this, active);
                if (!active) {
                    Log.d(Log.TAG_SYNC, "%s: Progress: !active", this);

                    if (!continuous) {
                        Log.d(Log.TAG_SYNC, "%s since !continuous, calling stopped()", this);
                        stop();
                    } else if (error != null) {
                        Log.d(Log.TAG_SYNC, "%s: Failed to xfer %d revisions, will retry in %d sec",
                                this,
                                revisionsFailed,
                                RETRY_DELAY);
                        cancelPendingRetryIfReady();
                        scheduleRetryIfReady();
                    } else {
                        Log.d(Log.TAG_SYNC, "%s since continuous and error == null, doing nothing", this);
                    }

                } else {
                    Log.d(Log.TAG_SYNC, "%s: Progress: active", this);
                }

            } else {
                Log.d(Log.TAG_SYNC, "%s: active == newActive.", this); 
            }
        } catch (Exception e) {
            Log.e(Log.TAG_SYNC, "Exception in updateActive()", e);
        } finally {
            Log.d(Log.TAG_SYNC, "%s: exit updateActive()", this);
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void addToInbox(RevisionInternal rev) {
        Log.v(Log.TAG_SYNC, "%s: addToInbox() called, rev: %s", this, rev);
        batcher.queueObject(rev);
        Log.v(Log.TAG_SYNC, "%s: addToInbox() calling updateActive()", this);
        updateActive();
    }

    @InterfaceAudience.Private
    protected void processInbox(RevisionList inbox) {

    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void sendAsyncRequest(String method, String relativePath, Object body, RemoteRequestCompletionBlock onCompletion) {
        try {
            String urlStr = buildRelativeURLString(relativePath);
            URL url = new URL(urlStr);
            sendAsyncRequest(method, url, body, onCompletion);
        } catch (MalformedURLException e) {
            Log.e(Log.TAG_SYNC, "Malformed URL for async request", e);
        }
    }

    @InterfaceAudience.Private
    /* package */ String buildRelativeURLString(String relativePath) {

        // the following code is a band-aid for a system problem in the codebase
        // where it is appending "relative paths" that start with a slash, eg:
        //     http://dotcom/db/ + /relpart == http://dotcom/db/relpart
        // which is not compatible with the way the java url concatonation works.

        String remoteUrlString = remote.toExternalForm();
        if (remoteUrlString.endsWith("/") && relativePath.startsWith("/")) {
            remoteUrlString = remoteUrlString.substring(0, remoteUrlString.length() - 1);
        }
        return remoteUrlString + relativePath;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void sendAsyncRequest(String method, URL url, Object body, final RemoteRequestCompletionBlock onCompletion) {

        final RemoteRequest request = new RemoteRequest(workExecutor, clientFactory, method, url, body, getLocalDatabase(), getHeaders(), onCompletion);

        request.setAuthenticator(getAuthenticator());

        request.setOnPreCompletion(new RemoteRequestCompletionBlock() {
            @Override
            public void onCompletion(Object result, Throwable e) {
                if (serverType == null && result instanceof HttpResponse) {
                    HttpResponse response = (HttpResponse) result;
                    Header serverHeader = response.getFirstHeader("Server");
                    if (serverHeader != null) {
                        String serverVersion = serverHeader.getValue();
                        Log.v(Log.TAG_SYNC, "serverVersion: %s", serverVersion);
                        serverType = serverVersion;
                    }
                }
            }
        });

        request.setOnPostCompletion(new RemoteRequestCompletionBlock() {
            @Override
            public void onCompletion(Object result, Throwable e) {
                requests.remove(request);
            }
        });


        if (remoteRequestExecutor.isTerminated()) {
            String msg = "sendAsyncRequest called, but remoteRequestExecutor has been terminated";
            throw new IllegalStateException(msg);
        }
        Future future = remoteRequestExecutor.submit(request);
        requests.put(request, future);

    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void sendAsyncMultipartDownloaderRequest(String method, String relativePath, Object body, Database db, RemoteRequestCompletionBlock onCompletion) {
        try {

            String urlStr = buildRelativeURLString(relativePath);
            URL url = new URL(urlStr);

            RemoteMultipartDownloaderRequest request = new RemoteMultipartDownloaderRequest(
                    workExecutor,
                    clientFactory,
                    method,
                    url,
                    body,
                    db,
                    getHeaders(),
                    onCompletion);

            request.setAuthenticator(getAuthenticator());

            remoteRequestExecutor.execute(request);
        } catch (MalformedURLException e) {
            Log.e(Log.TAG_SYNC, "Malformed URL for async request", e);
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void sendAsyncMultipartRequest(String method, String relativePath, MultipartEntity multiPartEntity, RemoteRequestCompletionBlock onCompletion) {
        URL url = null;
        try {
            String urlStr = buildRelativeURLString(relativePath);
            url = new URL(urlStr);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
        RemoteMultipartRequest request = new RemoteMultipartRequest(
                workExecutor,
                clientFactory,
                method,
                url,
                multiPartEntity,
                getLocalDatabase(),
                getHeaders(),
                onCompletion);

        request.setAuthenticator(getAuthenticator());

        remoteRequestExecutor.execute(request);
    }

    /**
     * CHECKPOINT STORAGE: *
     */

    @InterfaceAudience.Private
    /* package */ void maybeCreateRemoteDB() {
        // Pusher overrides this to implement the .createTarget option
    }

    /**
     * This is the _local document ID stored on the remote server to keep track of state.
     * Its ID is based on the local database ID (the private one, to make the result unguessable)
     * and the remote database's URL.
     *
     * @exclude
     */
    @InterfaceAudience.Private
    public String remoteCheckpointDocID() {

        if (remoteCheckpointDocID != null) {
            return remoteCheckpointDocID;
        } else {

            // TODO: Needs to be consistent with -hasSameSettingsAs: --
            // TODO: If a.remoteCheckpointID == b.remoteCheckpointID then [a hasSameSettingsAs: b]

            if (db == null) {
                return null;
            }

            // canonicalization: make sure it produces the same checkpoint id regardless of
            // ordering of filterparams / docids
            Map<String, Object> filterParamsCanonical = null;
            if (getFilterParams() != null) {
                filterParamsCanonical = new TreeMap<String, Object>(getFilterParams());
            }

            List<String> docIdsSorted = null;
            if (getDocIds() != null) {
                docIdsSorted = new ArrayList<String>(getDocIds());
                Collections.sort(docIdsSorted);
            }

            // use a treemap rather than a dictionary for purposes of canonicalization
            Map<String, Object> spec = new TreeMap<String, Object>();
            spec.put("localUUID", db.privateUUID());
            spec.put("remoteURL", remote.toExternalForm());
            spec.put("push", !isPull());
            spec.put("continuous", isContinuous());
	        spec.put("remote_db_uuid", getRemoteDbUuid());

            if (getFilter() != null) {
                spec.put("filter", getFilter());
            }
            if (filterParamsCanonical != null) {
                spec.put("filterParams", filterParamsCanonical);
            }
            if (docIdsSorted != null) {
                spec.put("docids", docIdsSorted);
            }

            byte[] inputBytes = null;
            try {
                inputBytes = db.getManager().getObjectMapper().writeValueAsBytes(spec);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            remoteCheckpointDocID = Misc.TDHexSHA1Digest(inputBytes);
            return remoteCheckpointDocID;

        }

    }

    @InterfaceAudience.Private
    private boolean is404(Throwable e) {
        if (e instanceof HttpResponseException) {
            return ((HttpResponseException) e).getStatusCode() == 404;
        }
        return false;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void fetchRemoteCheckpointDoc() {
        //lastSequenceChanged = false;
        String checkpointId = remoteCheckpointDocID();
        final String localLastSequence = db.lastSequenceWithCheckpointId(checkpointId);

        lastSequence = localLastSequence;
        Log.d(Log.TAG_SYNC, "%s: Replicating from lastSequence=%s", this, lastSequence);
        beginReplicating();
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void saveLastSequence() {
        if (remoteCheckpointDocID == null) {
            Log.w(Log.TAG_SYNC, "%s: remoteCheckpointDocID is null, aborting saveLastSequence()", this);
            return;
        }

        if (db != null) db.setLastSequence(lastSequence, remoteCheckpointDocID, !isPull());
    }

    @InterfaceAudience.Private
    private void stopRemoteRequests() {
        Log.v(Log.TAG_SYNC, "%s: stopRemoteRequests() cancelling: %d requests", this, requests.size());
        for (RemoteRequest request : requests.keySet()) {
            Log.v(Log.TAG_SYNC, "%s: aborting request: %s underlying req: %s", this, request, request.getRequest().getURI());
            request.abort();
            Log.v(Log.TAG_SYNC, "%s: aborted request", this);
        }
    }

    @InterfaceAudience.Private
    /* package */ void updateProgress() {
        if (!isRunning()) {
            status = ReplicationStatus.REPLICATION_STOPPED;
        } else {
            if (active) {
                status = ReplicationStatus.REPLICATION_ACTIVE;
            } else {
                status = ReplicationStatus.REPLICATION_IDLE;
            }
        }
    }

    @InterfaceAudience.Private
    protected void setError(Throwable throwable) {
        // TODO
        /*
        if (error.code == NSURLErrorCancelled && $equal(error.domain, NSURLErrorDomain))
            return;
         */

        if (throwable != error) {
            Log.e(Log.TAG_SYNC, "%s: Progress: set error = %s", this, throwable);
            error = throwable;
            notifyChangeListeners();
        }

    }

    @InterfaceAudience.Private
    protected void revisionFailed() {
        // Remember that some revisions failed to transfer, so we can later retry.
        ++revisionsFailed;
    }


    protected RevisionInternal transformRevision(RevisionInternal rev) {
        if(revisionBodyTransformationBlock != null) {
            try {
                final int generation = rev.getGeneration();
                RevisionInternal xformed = revisionBodyTransformationBlock.invoke(rev);
                if (xformed == null)
                    return null;
                if (xformed != rev) {
                    assert(xformed.getDocId().equals(rev.getDocId()));
                    assert(xformed.getRevId().equals(rev.getRevId()));
                    assert(xformed.getProperties().get("_revisions").equals(rev.getProperties().get("_revisions")));
                    if (xformed.getProperties().get("_attachments") != null) {
                        // Insert 'revpos' properties into any attachments added by the callback:
                        RevisionInternal mx = new RevisionInternal(xformed.getProperties(), db);
                        xformed = mx;
                        mx.mutateAttachments(new CollectionUtils.Functor<Map<String,Object>,Map<String,Object>>() {
                            public Map<String, Object> invoke(Map<String, Object> info) {
                                if (info.get("revpos") != null) {
                                    return info;
                                }
                                if(info.get("data") == null) {
                                    throw new IllegalStateException("Transformer added attachment without adding data");
                                }
                                Map<String,Object> nuInfo = new HashMap<String, Object>(info);
                                nuInfo.put("revpos",generation);
                                return nuInfo;
                            }
                        });
                    }
                    rev = xformed;
                }
            }catch (Exception e) {
                Log.w(Log.TAG_SYNC,"%s: Exception transforming a revision of doc '%s", e, this, rev.getDocId());
            }
        }
        return rev;
    }

    /**
     * Called after a <b> continuous </b> replication has gone idle, but it failed to transfer some revisions
     * and so wants to try again in a minute.
     *
     * <b> Should be overridden by subclasses. </b>
     */
    @InterfaceAudience.Private
    protected void retry() {
        setError(null);
    }

	/**
	 * @see #retry()
	 */
    @InterfaceAudience.Private
    protected void retryIfReady() {
        if (!running) {
            return;
        }
        Log.d(Log.TAG_SYNC, "%s: RETRYING, to transfer missed revisions", this);
        revisionsFailed = 0;
        cancelPendingRetryIfReady();
        retry();
    }

	/**
	 * @see #retry()
	 */
    @InterfaceAudience.Private
    protected void cancelPendingRetryIfReady() {
        if (retryIfReadyFuture != null && !retryIfReadyFuture.isCancelled()) {
            retryIfReadyFuture.cancel(true);
        }
    }

	/**
	 * @see #retry()
	 */
    @InterfaceAudience.Private
    protected void scheduleRetryIfReady() {
        retryIfReadyFuture = workExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                retryIfReady();
            }
        }, RETRY_DELAY, TimeUnit.SECONDS);
    }

    @InterfaceAudience.Private
    private int getStatusFromError(Throwable t) {
        if (t instanceof CouchbaseLiteException) {
            CouchbaseLiteException couchbaseLiteException = (CouchbaseLiteException) t;
            return couchbaseLiteException.getCBLStatus().getCode();
        }
        return Status.UNKNOWN;
    }

    @InterfaceAudience.Private
    protected Status statusFromBulkDocsResponseItem(Map<String, Object> item) {

        try {
            if (!item.containsKey("error")) {
                return new Status(Status.OK);
            }
            String errorStr = (String) item.get("error");
            if (errorStr == null || errorStr.isEmpty()) {
                return new Status(Status.OK);
            }

            // 'status' property is nonstandard; TouchDB returns it, others don't.
            String statusString = (String) item.get("status");
            int status = Integer.parseInt(statusString);
            if (status >= 400) {
                return new Status(status);
            }
            // If no 'status' present, interpret magic hardcoded CouchDB error strings:
            if (errorStr.equalsIgnoreCase("unauthorized")) {
                return new Status(Status.UNAUTHORIZED);
            } else if (errorStr.equalsIgnoreCase("forbidden")) {
                return new Status(Status.FORBIDDEN);
            } else if (errorStr.equalsIgnoreCase("conflict")) {
                return new Status(Status.CONFLICT);
            } else {
                return new Status(Status.UPSTREAM_ERROR);
            }

        } catch (Exception e) {
            Log.e(Database.TAG, "Exception getting status from " + item, e);
        }
        return new Status(Status.OK);


    }

    @InterfaceAudience.Private
    /* package */ void setServerType(String serverType) {
        this.serverType = serverType;
    }

    @InterfaceAudience.Private
    /* package */ HttpClientFactory getClientFactory() {
        return clientFactory;
    }

    @InterfaceAudience.Private
    /* package */ void setRetryDelay(int retryDelay) {
        Replication.RETRY_DELAY = retryDelay;
    }


}
