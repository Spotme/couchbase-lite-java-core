package com.couchbase.lite;

import com.couchbase.lite.auth.Authorizer;
import com.couchbase.lite.auth.FacebookAuthorizer;
import com.couchbase.lite.auth.PersonaAuthorizer;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.replicator.Puller;
import com.couchbase.lite.replicator.Pusher;
import com.couchbase.lite.replicator.Replication;
import com.couchbase.lite.spotme.DbCorruptionHandler;
import com.couchbase.lite.support.FileDirUtils;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.Version;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.StreamUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Top-level CouchbaseLite object; manages a collection of databases as a CouchDB server does.
 */
public final class Manager {

    /**
     * @exclude
     */
    public static final String HTTP_ERROR_DOMAIN =  "CBLHTTP";

    /**
     * @exclude
     */
    public static final String DATABASE_SUFFIX_OLD = ".touchdb";

    /**
     * @exclude
     */
    public static final String DATABASE_SUFFIX = ".cblite";

    /**
     * @exclude
     */
    public static final ManagerOptions DEFAULT_OPTIONS = new ManagerOptions();

    /**
     * @exclude
     */
    public static final String LEGAL_CHARACTERS = "[^a-z]{1,}[^a-z0-9_$()/+-]*$";

    public static final String VERSION = Version.VERSION;

    private static final ObjectMapper mapper = new ObjectMapper();
    private static ObjectReader mapReader;
    private ManagerOptions options;
    private File directoryFile;
    private Map<String, Database> databases;
    private List<Replication> replications;
    private ScheduledExecutorService workExecutor;
    private HttpClientFactory defaultHttpClientFactory;
    private Context context;
    private DbCorruptionHandler dbCorruptionHandler;

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public static ObjectMapper getObjectMapper() {
        return mapper;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public static ObjectReader getMapReader() {
        if (mapReader == null) mapReader = mapper.reader(Map.class);
        return mapReader;
    }

    /**
     * Constructor
     * @throws UnsupportedOperationException - not currently supported
     * @exclude
     */
    @InterfaceAudience.Public
    public Manager() {
        final String detailMessage = "Parameterless constructor is not a valid API call on Android. " +
                " Pure java version coming soon.";
        throw new UnsupportedOperationException(detailMessage);
    }

    public static void enableAllWarnLogs() {
        Log.enableAllWarnLogs();
    }

    public static void disableAllLogs() {
        //
        Log.disableAllLogs();
    }

    /**
     * Enable logging for a particular tag / loglevel combo
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param logLevel The loglevel to enable.  Anything matching this loglevel
     *                 or having a more urgent loglevel will be emitted.  Eg, Log.VERBOSE.
     */
    public static void enableLogging(String tag, int logLevel) {
        Log.enableLogging(tag, logLevel);
    }

    /**
     * Constructor
     *
     * @throws java.lang.SecurityException - Runtime exception that can be thrown by File.mkdirs()
     */
    @InterfaceAudience.Public
    public Manager(Context context, ManagerOptions options) throws IOException {

        //IMPORTANT !!!  DO NOT CALL Log.i() method here as it will block current thread for logger loading.
//        Log.i(Database.TAG, "Starting Manager version: %s", Manager.VERSION);

        this.context = context;

        this.directoryFile = (options != null && options.getDatabaseDir() != null)
                ? options.getDatabaseDir()
                : context.getFilesDir();

        this.options = (options != null) ? options : DEFAULT_OPTIONS;
        this.databases = new ConcurrentHashMap<>();
        this.replications = new ArrayList<Replication>();

        directoryFile.mkdirs();
        if (!directoryFile.isDirectory()) {
            throw new IOException(String.format("Unable to create directory for: %s", directoryFile));
        }

        // Useless in our case
//        upgradeOldDatabaseFiles(directoryFile);
        workExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Get shared instance
     * @throws UnsupportedOperationException - not currently supported
     * @exclude
     */
    @InterfaceAudience.Public
    public static Manager getSharedInstance() {
        final String detailMessage = "getSharedInstance() is not a valid API call on Android. " +
                " Pure java version coming soon";
        throw new UnsupportedOperationException(detailMessage);
    }

    /**
     * Returns YES if the given name is a valid database name.
     * (Only the characters in "abcdefghijklmnopqrstuvwxyz0123456789_$()+-/" are allowed.)
     */
    @InterfaceAudience.Public
    public static boolean isValidDatabaseName(String databaseName) {
        if (databaseName.length() > 0 && databaseName.length() < 240 &&
                containsOnlyLegalCharacters(databaseName) &&
                Character.isLowerCase(databaseName.charAt(0))) {
            return true;
        }
        return databaseName.equals(Replication.REPLICATOR_DATABASE_NAME);
    }

    /**
     * The root directory of this manager (as specified at initialization time.)
     */
    @InterfaceAudience.Public
    public File getDirectory() {
        return directoryFile;
    }

    /**
     * An array of the names of all existing databases.
     */
    @InterfaceAudience.Public
    public List<String> getAllDatabaseNames() {
        String[] databaseFiles = directoryFile.list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String filename) {
                if(filename.endsWith(Manager.DATABASE_SUFFIX)) {
                    return true;
                }
                return false;
            }
        });
        List<String> result = new ArrayList<String>();
        for (String databaseFile : databaseFiles) {
            String trimmed = databaseFile.substring(0, databaseFile.length() - Manager.DATABASE_SUFFIX.length());
            String replaced = trimmed.replace(':', '/');
            result.add(replaced);
        }
        Collections.sort(result);
        return Collections.unmodifiableList(result);
    }

    /**
     * Releases all resources used by the Manager instance and closes all its databases.
     */
    @InterfaceAudience.Public
    public void close() {
        Log.i(Database.TAG, "Closing " + this);
        for (Database database : databases.values()) {
            List<Replication> replicators = database.getAllReplications();
            if (replicators != null) {
                for (Replication replicator : replicators) {
                    replicator.stop();
                }
            }
            database.close();
        }
        databases.clear();
        Log.i(Database.TAG, "Closed " + this);
    }


    //todo add back "throws CouchbaseLiteException" instead of silently returning null
    /**
     * Returns open database with the given name, or creates it if it doesn't exist.
     * Multiple calls with the same name will return the same Database instance.
     *
     * @throws IllegalStateException if unable to create a storage engine.
     * E.g. encryption key is wrong.
     */
    @InterfaceAudience.Public
    public Database getDatabase(String name) {
        final boolean mustExist = false;

        return getDatabaseInner(name, mustExist);
    }

    /**
     * Returns the database with the given name, or null if it doesn't exist.
     * Multiple calls with the same name will return the same Database instance.
     *
     * @return open db or null if it's not possible to open existing db.
     * @throws IllegalStateException if storage engine was not created. E.g. wrong db encryption, etc.
     *
     */
    @InterfaceAudience.Public
    public Database getExistingDatabase(String name) throws CouchbaseLiteException {
        final boolean mustExist = true;

        return getDatabaseInner(name, mustExist);
    }


    /**
     * Gets db as encrypted db. If opening is failed - trys to get it as not encrypted db.
     *
     * @throws IllegalStateException if unable to create a storage engine.
     * E.g. encryption key is wrong.
     */
    private synchronized Database getDatabaseInner(String name, boolean mustExist) {
        Database db = databases.get(name);
        if(db == null) {

            //open as encrypted db
            db = getDatabaseWithoutOpeningWithoutCaching(name, mustExist, null);
            if (db != null) {
                try {
                    boolean opened = db.open();
                    if (!opened) {
                        return null;
                    }
                } catch (IllegalStateException e) {
                    //AndroidSQLiteStorageEngine.TAG is not accessible in java-core submodule
                    Log.w("AndroidSQLiteStorageEngine", "Unable to open db " + name + " as encrypted. Trying without encryption", e);

                    //open as non encrypted db
                    db = getDatabaseWithoutOpeningWithoutCaching(name, mustExist, "");
                    if (db != null) {
                        boolean opened = db.open();
                        //AndroidSQLiteStorageEngine.TAG is not accessible in java-core submodule
                        Log.w("AndroidSQLiteStorageEngine", "Successfully open db " + name + " as non-encrypted. Opened status: " + opened);
                        if (!opened) {
                            return null;
                        }
                    }
                }
            }
            databases.put(name, db);
        }

        return db;
    }


    /**
     * Replaces or installs a database from a file.
     *
     * This is primarily used to install a canned database on first launch of an app, in which case
     * you should first check .exists to avoid replacing the database if it exists already. The
     * canned database would have been copied into your app bundle at build time.
     *
     * @param databaseName  The name of the target Database to replace or create.
     * @param databaseStream  InputStream on the source Database file.
     * @param attachmentStreams  Map of the associated source Attachments, or null if there are no attachments.
     *                           The Map key is the name of the attachment, the map value is an InputStream for
     *                           the attachment contents. If you wish to control the order that the attachments
     *                           will be processed, use a LinkedHashMap, SortedMap or similar and the iteration order
     *                           will be honoured.
     **/
    @InterfaceAudience.Public
    public void replaceDatabase(String databaseName, InputStream databaseStream, Map<String,InputStream> attachmentStreams) throws CouchbaseLiteException {
        replaceDatabase(databaseName, databaseStream, attachmentStreams == null ? null : attachmentStreams.entrySet().iterator());
    }

    private void replaceDatabase(String databaseName, InputStream databaseStream, Iterator<Map.Entry<String,InputStream>> attachmentStreams) throws CouchbaseLiteException {
        try {
            Database database = getDatabase(databaseName);
            String dstAttachmentsPath = database.getAttachmentStorePath();
            OutputStream destStream = new FileOutputStream(new File(database.getPath()));
            StreamUtils.copyStream(databaseStream, destStream);
            File attachmentsFile = new File(dstAttachmentsPath);
            FileDirUtils.deleteRecursive(attachmentsFile);
            attachmentsFile.mkdirs();
            if(attachmentStreams != null) {
                StreamUtils.copyStreamsToFolder(attachmentStreams,attachmentsFile);
            }

            database.open();
            database.replaceUUIDs();
        }
        catch (FileNotFoundException e) {
            Log.e(Database.TAG, "", e);
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        }
        catch (IOException e) {
            Log.e(Database.TAG, "", e);
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        }

    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public HttpClientFactory getDefaultHttpClientFactory() {
        return defaultHttpClientFactory;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void setDefaultHttpClientFactory(
            HttpClientFactory defaultHttpClientFactory) {
        this.defaultHttpClientFactory = defaultHttpClientFactory;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    private static boolean containsOnlyLegalCharacters(String databaseName) {
        Pattern p = Pattern.compile("^[abcdefghijklmnopqrstuvwxyz0123456789_$()+-/]+$");
        Matcher matcher = p.matcher(databaseName);
        return matcher.matches();
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    private void upgradeOldDatabaseFiles(File directory) {
        File[] files = directory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File file, String name) {
                return name.endsWith(DATABASE_SUFFIX_OLD);
            }
        });

        for (File file : files) {
            String oldFilename = file.getName();
            String newFilename = filenameWithNewExtension(oldFilename, DATABASE_SUFFIX_OLD, DATABASE_SUFFIX);
            File newFile = new File(directory, newFilename);
            if (newFile.exists()) {
                Log.w(Database.TAG, "Cannot rename %s to %s, %s already exists", oldFilename, newFilename, newFilename);
                continue;
            }
            boolean ok = file.renameTo(newFile);
            if (!ok) {
                String msg = String.format("Unable to rename %s to %s", oldFilename, newFilename);
                throw new IllegalStateException(msg);
            }
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    private String filenameWithNewExtension(String oldFilename, String oldExtension, String newExtension) {
        String oldExtensionRegex = String.format("%s$",oldExtension);
        return oldFilename.replaceAll(oldExtensionRegex, newExtension);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Collection<Database> allOpenDatabases() {
        return databases.values();
    }


    /**
     * Asynchronously dispatches a callback to run on a background thread. The callback will be passed
     * Database instance.  There is not currently a known reason to use it, it may not make
     * sense on the Android API, but it was added for the purpose of having a consistent API with iOS.
     * @exclude
     */
    @InterfaceAudience.Private
    public Future runAsync(String databaseName, final AsyncTask function) throws CouchbaseLiteException {

        final Database database = getDatabase(databaseName);
        return runAsync(new Runnable() {
            @Override
            public void run() {
                function.run(database);
            }
        });

    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    Future runAsync(Runnable runnable) {
        return workExecutor.submit(runnable);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    private String pathForName(String name) {
        if((name == null) || (name.length() == 0) || Pattern.matches(LEGAL_CHARACTERS, name)) {
            return null;
        }
        name = name.replace('/', ':');
        String result = directoryFile.getPath() + File.separator + name + Manager.DATABASE_SUFFIX;
        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    private Map<String, Object> parseSourceOrTarget(Map<String,Object> properties, String key) {
        Map<String, Object> result = new HashMap<String, Object>();

        Object value = properties.get(key);

        if (value instanceof String) {
            result.put("url", (String)value);
        }
        else if (value instanceof Map) {
            result = (Map<String, Object>) value;
        }
        return result;

    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    Replication replicationWithDatabase(Database db, URL remote, String remoteDbUuid, boolean push, boolean create, boolean start) {
        for (Replication replicator : replications) {
            if (replicator.getLocalDatabase() == db && replicator.getRemoteUrl().equals(remote) && replicator.isPull() == !push) {
                return replicator;
            }

        }
        if (!create) {
            return null;
        }

        Replication replicator = null;
        final boolean continuous = false;

        if (push) {
            replicator = new Pusher(db, remote, null, remoteDbUuid, continuous, getWorkExecutor());
        }
        else {
            replicator = new Puller(db, remote, null, remoteDbUuid, continuous, false, false, 200, getWorkExecutor());
        }

        replications.add(replicator);
        if (start) {
            replicator.start();
        }

        return replicator;
    }

    /**
     * @return db, which is neither taken from {@link #databases}, nor stored there after construction of the db object.
     * or null if db mustExist, but it's not.
     *
     * @exclude
     */
    @InterfaceAudience.Private
    public synchronized Database getDatabaseWithoutOpeningWithoutCaching(String name, boolean mustExist, String dbPassword) throws IllegalArgumentException {
        if (!isValidDatabaseName(name)) {
            throw new IllegalArgumentException("Invalid database name: " + name);
        }
        if (options.isReadOnly()) {
            mustExist = true;
        }
        String path = pathForName(name);
        if (path == null) {
            return null;
        }
        Database db  = new Database(path, this, dbPassword == null ? options.getDatabasePassword() : dbPassword);
        if (mustExist && !db.exists()) {
            Log.w(Database.TAG, "mustExist is true and db (%s) does not exist", name);
            return null;
        }
        db.setName(name);
        //handler must be set before calling .open() (as it passed to inner sqlcipher db there)
        db.setDbCorruptionHandler(dbCorruptionHandler);

        return db;
    }

    /**
     * Get either already open db of the "cache" of the open dbs or construct new object of Not opened db.
     *
     *  Encryption key does not matter for not opened db as it will be deleted.
     *
     * @exclude
     */
    @InterfaceAudience.Private
    public synchronized Database getDatabaseWithoutOpeningForDeletion(String name) {
        Database db = databases.get(name);
        if(db == null) {
            if (!isValidDatabaseName(name)) {
                throw new IllegalArgumentException("Invalid database name: " + name);
            }
            String path = pathForName(name);
            if (path == null) {
                return null;
            }
            db = new Database(path, this, options.getDatabasePassword());
            if (!db.exists()) {
                Log.w(Database.TAG, "mustExist is true and db (%s) does not exist", name);
                return null;
            }
            db.setName(name);
            //we can put it to the list. It will be removed on further db.delete()
            databases.put(name, db);
        }
        return db;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Public
    public void forgetDatabase(Database db) {

        // remove from cached list of dbs
        databases.remove(db.getName());

        // remove from list of replications
        // TODO: should there be something that actually stops the replication(s) first?
        Iterator<Replication> replicationIterator = this.replications.iterator();
        while (replicationIterator.hasNext()) {
            Replication replication = replicationIterator.next();
            if (replication.getLocalDatabase().getName().equals(db.getName())) {
                replicationIterator.remove();
            }
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Replication getReplicator(Map<String,Object> properties) throws CouchbaseLiteException {

        // TODO: in the iOS equivalent of this code, there is: {@"doc_ids", _documentIDs}) - write unit test that detects this bug
        // TODO: ditto for "headers"

        Authorizer authorizer = null;
        Replication repl = null;
        URL remote = null;

        Map<String, Object> remoteMap;

        Map<String, Object> sourceMap = parseSourceOrTarget(properties, "source");
        Map<String, Object> targetMap = parseSourceOrTarget(properties, "target");

        String source = (String)sourceMap.get("url");
        String target = (String)targetMap.get("url");

	    String remoteDbUuid = (String)properties.get("node_db_uuid");

        Boolean createTargetBoolean = (Boolean)properties.get("create_target");
        boolean createTarget = (createTargetBoolean != null && createTargetBoolean.booleanValue());

        Boolean bulkGetBoolean = (Boolean)properties.get("bulk_get");
        boolean bulkGet = (bulkGetBoolean != null && bulkGetBoolean.booleanValue());

        Boolean ignoreRemovedBoolean = (Boolean)properties.get("ignore_removed");
        boolean ignoreRemoved = (ignoreRemovedBoolean != null && ignoreRemovedBoolean.booleanValue());

        int batchSize;
        try {
            batchSize = (Integer)properties.get("batch_size");
        } catch (Exception e){
            batchSize = 200;
        }
        if (batchSize == 0) batchSize = 200;

        Boolean continuousBoolean = (Boolean)properties.get("continuous");
        boolean continuous = (continuousBoolean != null && continuousBoolean.booleanValue());

        Boolean cancelBoolean = (Boolean)properties.get("cancel");
        boolean cancel = (cancelBoolean != null && cancelBoolean.booleanValue());

	    List <String> docsToReplicate = (List <String>) properties.get("doc_ids");

	    String replicationID = (String) properties.get("_replication_id");

        // Map the 'source' and 'target' JSON params to a local database and remote URL:
        if(source == null || target == null) {
            throw new CouchbaseLiteException("source and target are both null", new Status(Status.BAD_REQUEST));
        }

        boolean push = false;
        Database db = null;
        String remoteStr = null;

        if (Manager.isValidDatabaseName(source)) {
            db = getExistingDatabase(source);
            remoteStr = target;
            push = true;
            remoteMap = targetMap;
        } else {
            remoteStr = source;
            if(createTarget && !cancel) {
                db = getDatabase(target);
                //todo replace with throw from method
                if(db == null) {
                    throw new CouchbaseLiteException("cannot open database: " + db, new Status(Status.INTERNAL_SERVER_ERROR));
                }
            } else {
                db = getExistingDatabase(target);
            }
            if(db == null) {
                throw new CouchbaseLiteException("database is null", new Status(Status.NOT_FOUND));
            }
            remoteMap = sourceMap;

        }


        Map<String, Object> authMap = (Map<String, Object>) remoteMap.get("auth");
        if (authMap != null) {

            Map<String, Object> persona = (Map<String, Object>) authMap.get("persona");
            if (persona != null) {
                String email = (String) persona.get("email");
                authorizer = new PersonaAuthorizer(email);
            }
            Map<String, Object> facebook = (Map<String, Object>) authMap.get("facebook");
            if (facebook != null) {
                String email = (String) facebook.get("email");
                authorizer = new FacebookAuthorizer(email);
            }

        }

        try {
            remote = new URL(remoteStr);
        } catch (MalformedURLException e) {
            throw new CouchbaseLiteException("malformed remote url: " + remoteStr, new Status(Status.BAD_REQUEST));
        }
        if(remote == null) {
            throw new CouchbaseLiteException("remote URL is null: " + remoteStr, new Status(Status.BAD_REQUEST));
        }


        if(!cancel) {
            repl = db.getReplicator(remote, replicationID, remoteDbUuid, getDefaultHttpClientFactory(), push, continuous, bulkGet, ignoreRemoved, batchSize, getWorkExecutor());
            if(repl == null) {
                throw new CouchbaseLiteException("unable to create replicator with remote: " + remote, new Status(Status.INTERNAL_SERVER_ERROR));
            }

            if (authorizer != null) {
                repl.setAuthenticator(authorizer);
            }

            Map<String, Object> headers = (Map) properties.get("headers");
            if (headers == null) headers = new HashMap<String, Object>();
            if (remoteMap != null && remoteMap.containsKey("headers")) {
                Map<String, String> sourceHeaders = (Map<String, String>) remoteMap.get("headers");
                headers.putAll(sourceHeaders);
            }
            if (headers != null && !headers.isEmpty()) {
                repl.setHeaders(headers);
            }

            String filterName = (String)properties.get("filter");
            if(filterName != null) {
                repl.setFilter(filterName);
                Map<String,Object> filterParams = (Map<String,Object>)properties.get("query_params");
                if(filterParams != null) {
                    repl.setFilterParams(filterParams);
                }
            }

	        if (docsToReplicate != null && !docsToReplicate.isEmpty()) {
		        repl.setDocIds(docsToReplicate);
	        }

            if(push) {
                ((Pusher)repl).setCreateTarget(createTarget);
            }


        } else {
            // Cancel replication:
            repl = db.getActiveReplicator(replicationID, remote, remoteDbUuid, push);
            if(repl == null) {
                throw new CouchbaseLiteException("unable to lookup replicator with remote: " + remote, new Status(Status.NOT_FOUND));
            }
        }

        return repl;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public ScheduledExecutorService getWorkExecutor() {
        return workExecutor;
    }

    @InterfaceAudience.Private
    public Context getContext() {
        return context;
    }

    public void setDbCorruptionHandler(DbCorruptionHandler dbCorruptionHandler) {
        this.dbCorruptionHandler = dbCorruptionHandler;
    }

    /**
     * Should be used only in internal logic of re-encryption non-encrypted dbs.
     */
    /* package */ String getDatabasePassword() {
        return options.getDatabasePassword();
    }
}

