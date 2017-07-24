/**
 * Original iOS version by  Jens Alfke
 * Ported to Android by Marty Schoch
 *
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.couchbase.lite;


import com.couchbase.lite.Database.TDContentOptions;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.storage.ContentValues;
import com.couchbase.lite.storage.Cursor;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.storage.SQLiteStorageEngine;
import com.couchbase.lite.support.JsonDocument;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Represents a view available in a database.
 */
public final class View {

    /**
     * @exclude
     */
    public static final int REDUCE_BATCH_SIZE = 100;

    /*
     * Avoid to load attachments for views if not needed
     */
    private boolean skipAttachments;

    /**
     * @exclude
     */
    public enum TDViewCollation {
        TDViewCollationUnicode, TDViewCollationRaw, TDViewCollationASCII
    }

    private Database database;
    private String name;
    private int viewId;
    private Mapper mapBlock;
    private Reducer reduceBlock;
    private TDViewCollation collation;
    private static ViewCompiler viewCompiler;
	private static FunctionCompiler functionCompiler;

    /**
     * The registered object, if any, that can compile map/reduce functions from source code.
     */
    @InterfaceAudience.Public
    public static ViewCompiler getViewCompiler() {
        return viewCompiler;
    }

    /**
     * Registers an object that can compile map/reduce functions from source code.
     */
    @InterfaceAudience.Public
    public static void setViewCompiler(ViewCompiler compiler) {
        View.viewCompiler = compiler;
    }

	/**
	 * A new instance of the registered object, if any, that can compile map/reduce functions from source code.
	 */
	@InterfaceAudience.Public
	public static FunctionCompiler getFunctionCompiler() {
		return (functionCompiler != null) ? functionCompiler.newInstance() : null;
	}

	/**
	 * Registers an object that can compile map/reduce functions from source code.
	 */
	@InterfaceAudience.Public
	public static void setFunctionCompiler(FunctionCompiler compiler) {
		View.functionCompiler = compiler;
	}


    /**
     * Constructor
     */
    @InterfaceAudience.Private
    /* package */ View(Database database, String name) {
        this.database = database;
        this.name = name;
        this.viewId = -1; // means 'unknown'
        this.collation = TDViewCollation.TDViewCollationUnicode;
    }

    /**
     * Get the database that owns this view.
     */
    @InterfaceAudience.Public
    public Database getDatabase() {
        return database;
    };

    /**
     * Get the name of the view.
     */
    @InterfaceAudience.Public
    public String getName() {
        return name;
    }

    /**
     * The map function that controls how index rows are created from documents.
     */
    @InterfaceAudience.Public
    public Mapper getMap() {
        return mapBlock;
    }

    /**
     * The optional reduce function, which aggregates together multiple rows.
     */
    @InterfaceAudience.Public
    public Reducer getReduce() {
        return reduceBlock;
    }


    /**
     * Get document type.
     *
     * The document "type" property values this view is filtered to (nil if none.)
     */
    public Collection<String> getDocumentTypes() {
        return database.getViewDocumentTypes(name);
    }

    /**
     * Set document type. If the document type is set, only documents whose "type" property
     * is equal to its value will be passed to the map block and indexed. This can speed up indexing.
     * Just like the map block, this property is not persistent; it needs to be set at runtime before
     * the view is queried. And if its value changes, the view's version also needs to change.
     * @param docType
     */
    public void setDocumentType(Collection<String> docType) {
        database.setViewDocumentTypes(docType, name);
    }

    public void setSkipAttachments(boolean skipAttachments) {
        this.skipAttachments = skipAttachments;
    }

    /**
     * Is the view's index currently out of date?
     */
    @InterfaceAudience.Public
    public boolean isStale() {
        return (getLastSequenceIndexed() < database.getLastSequenceNumber());
    }

    /**
     * Get the last sequence number indexed so far.
     */
    @InterfaceAudience.Public
    public long getLastSequenceIndexed() {
        String sql = "SELECT lastSequence FROM views WHERE name=?";
        String[] args = { name };
        Cursor cursor = null;
        long result = -1;
        try {
            cursor = database.getDatabase().rawQuery(sql, args);
            if (cursor.moveToNext()) {
                result = cursor.getLong(0);
            }
        } catch (Exception e) {
            Log.e(Log.TAG_VIEW, "Error getting last sequence indexed", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    /**
     * Defines a view that has no reduce function.
     * See setMapReduce() for more information.
     */
    @InterfaceAudience.Public
    public boolean setMap(Mapper mapBlock, String version) {
        return setMapReduce(mapBlock, null, version);
    }

    /**
     * Defines a view's functions.
     *
     * The view's definition is given as a class that conforms to the Mapper or
     * Reducer interface (or null to delete the view). The body of the block
     * should call the 'emit' object (passed in as a paramter) for every key/value pair
     * it wants to write to the view.
     *
     * Since the function itself is obviously not stored in the database (only a unique
     * string idenfitying it), you must re-define the view on every launch of the app!
     * If the database needs to rebuild the view but the function hasn't been defined yet,
     * it will fail and the view will be empty, causing weird problems later on.
     *
     * It is very important that this block be a law-abiding map function! As in other
     * languages, it must be a "pure" function, with no side effects, that always emits
     * the same values given the same input document. That means that it should not access
     * or change any external state; be careful, since callbacks make that so easy that you
     * might do it inadvertently!  The callback may be called on any thread, or on
     * multiple threads simultaneously. This won't be a problem if the code is "pure" as
     * described above, since it will as a consequence also be thread-safe.
     *
     */
    @InterfaceAudience.Public
    public boolean setMapReduce(Mapper mapBlock,
                                Reducer reduceBlock, String version) {
        assert (mapBlock != null);
        assert (version != null);

        this.mapBlock = mapBlock;
        this.reduceBlock = reduceBlock;

        if(!database.open()) {
            return false;
        }

        // Update the version column in the database. This is a little weird looking
        // because we want to
        // avoid modifying the database if the version didn't change, and because the
        // row might not exist yet.

        SQLiteStorageEngine storageEngine = this.database.getDatabase();

        // Older Android doesnt have reliable insert or ignore, will to 2 step
        // FIXME review need for change to execSQL, manual call to changes()

        String sql = "SELECT name, version FROM views WHERE name=?";
        String[] args = { name };
        Cursor cursor = null;

        try {
            cursor = storageEngine.rawQuery(sql, args);
            if (!cursor.moveToNext()) {
                // no such record, so insert
                ContentValues insertValues = new ContentValues();
                insertValues.put("name", name);
                insertValues.put("version", version);
                storageEngine.insert("views", null, insertValues);
                return true;
            }

            ContentValues updateValues = new ContentValues();
            updateValues.put("version", version);
            updateValues.put("lastSequence", 0);

            String[] whereArgs = { name, version };
            int rowsAffected = storageEngine.update("views", updateValues,
                    "name=? AND version!=?", whereArgs);

            return (rowsAffected > 0);
        } catch (SQLException e) {
            Log.e(Log.TAG_VIEW, "Error setting map block", e);
            return false;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

    }


    /**
     * Deletes the view's persistent index. It will be regenerated on the next query.
     */
    @InterfaceAudience.Public
    public void deleteIndex() {
        if (getViewId() < 0) {
            return;
        }

        boolean success = false;
        try {
            database.beginTransaction();

            String[] whereArgs = { Integer.toString(getViewId()) };
            database.getDatabase().delete("maps", "view_id=?", whereArgs);

            ContentValues updateValues = new ContentValues();
            updateValues.put("lastSequence", 0);
            database.getDatabase().update("views", updateValues, "view_id=?",
                    whereArgs);

            success = true;
        } catch (SQLException e) {
            Log.e(Log.TAG_VIEW, "Error removing index", e);
        } finally {
            database.endTransaction(success);
        }
    }

    /**
     * Deletes the view, persistently.
     */
    @InterfaceAudience.Public
    public void delete() {
        database.deleteViewNamed(name);
        viewId = 0;
    }

    /**
     * Creates a new query object for this view. The query can be customized and then executed.
     */
    @InterfaceAudience.Public
    public Query createQuery() {
        return new Query(getDatabase(), this);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public int getViewId() {
        if (viewId < 0) {
            String sql = "SELECT view_id FROM views WHERE name=?";
            String[] args = { name };
            Cursor cursor = null;
            try {
                cursor = database.getDatabase().rawQuery(sql, args);
                if (cursor.moveToNext()) {
                    viewId = cursor.getInt(0);
                } else {
                    viewId = 0;
                }
            } catch (SQLException e) {
                Log.e(Log.TAG_VIEW, "Error getting view id", e);
                viewId = 0;
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
            }
        }
        return viewId;
    }

    /**
     * in CBLView.m
     * - (NSUInteger) totalRows
     */
    @InterfaceAudience.Private
    public int getTotalRows(){

        int totalRows = -1;
        String sql = "SELECT total_docs FROM views WHERE view_id=?";
        String[] args = { String.valueOf(viewId) };
        Cursor cursor = null;
        try {
            cursor = database.getDatabase().rawQuery(sql, args);
            if (cursor.moveToNext()) {
                totalRows = cursor.getInt(0);
            }
        } catch (SQLException e) {
            Log.e(Log.TAG_VIEW, "Error getting total_docs", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        // need to count & update rows
        if(totalRows < 0){
            totalRows = countTotalRows();
            if(totalRows >= 0){
                updateTotalRows(totalRows);
            }
        }
        return totalRows;
    }

    private int countTotalRows(){
        int totalRows = -1;
        String sql = "SELECT COUNT(view_id) FROM maps WHERE view_id=?";
        String[] args = { String.valueOf(viewId) };
        Cursor cursor = null;
        try {
            cursor = database.getDatabase().rawQuery(sql, args);
            if (cursor.moveToNext()) {
                totalRows = cursor.getInt(0);
            }
        } catch (SQLException e) {
            Log.e(Log.TAG_VIEW, "Error getting total_docs", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return totalRows;
    }

    private void updateTotalRows(int totalRows){
        ContentValues values = new ContentValues();
        values.put("total_docs=", totalRows);
        database.getDatabase().update("views", values, "view_id=?", new String[]{ String.valueOf(viewId) });
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void databaseClosing() {
        database = null;
        viewId = 0;
    }

    /*** Indexing ***/

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String toJSONString(Object object) {
        if (object == null) {
            return null;
        }
        String result = null;
        try {
            result = Manager.getObjectMapper().writeValueAsString(object);
        } catch (Exception e) {
            Log.w(Log.TAG_VIEW, "Exception serializing object to json: %s", e, object);
        }
        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public TDViewCollation getCollation() {
        return collation;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void setCollation(TDViewCollation collation) {
        this.collation = collation;
    }

    int added = 0;

    //TODO: Avoid doing another "view calculation"  of the same view if it's already in progress: use lock per "view name"
    /**
     * Updates the view's index (incrementally) if necessary.
     * @return 200 if updated, 304 if already up-to-date, else an error code
     * @exclude
     */
    @SuppressWarnings("unchecked")
    @InterfaceAudience.Private
    public void updateIndex() throws CouchbaseLiteException {
        Date d1 = new Date();
        Log.v(Log.TAG_VIEW, "Re-indexing view: %s", name);
        assert (mapBlock != null);

        if (getViewId() <= 0) {
            String msg = String.format("getViewId() < 0");
            throw new CouchbaseLiteException(msg, new Status(Status.NOT_FOUND));
        }

        database.beginTransaction();
        Status result = new Status(Status.INTERNAL_SERVER_ERROR);
        Cursor cursor = null;

        ExecutorService taskExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        final List<ContentValues> inserts = Collections.synchronizedList(new ArrayList());

        try {
            long lastSequence = getLastSequenceIndexed();
            long dbMaxSequence = database.getLastSequenceNumber();
            if(lastSequence == dbMaxSequence) {
                // nothing to do (eg,  kCBLStatusNotModified)
                Log.v(Log.TAG_VIEW, "lastSequence (%s) == dbMaxSequence (%s), nothing to do",
                        lastSequence, dbMaxSequence);
                result.setCode(Status.NOT_MODIFIED);
                return;
            }

            // First remove obsolete emitted results from the 'maps' table:
            long sequence = lastSequence;
            if (lastSequence < 0) {
                String msg = String.format("lastSequence < 0 (%s)", lastSequence);
                throw new CouchbaseLiteException(msg, new Status(Status.INTERNAL_SERVER_ERROR));
            }

            if (lastSequence == 0) {
                // If the lastSequence has been reset to 0, make sure to remove
                // any leftover rows:
                String[] whereArgs = { Integer.toString(getViewId()) };
                database.getDatabase().delete("maps", "view_id=?", whereArgs);
            } else {
                database.optimizeSQLIndexes();
                // Delete all obsolete map results (ones from since-replaced
                // revisions):
                String[] args = { Integer.toString(getViewId()),
                        Long.toString(lastSequence),
                        Long.toString(lastSequence) };
                database.getDatabase().execSQL(
                        "DELETE FROM maps WHERE view_id=? AND sequence IN ("
                                + "SELECT parent FROM revs WHERE sequence>? "
                                + "AND +parent>0 AND +parent<=?)", args);
            }

            int deleted = 0;
            added = 0;
            cursor = database.getDatabase().rawQuery("SELECT changes()", null);
            cursor.moveToNext();
            deleted = cursor.getInt(0);
            cursor.close();

            // This is the emit() block, which gets called from within the
            // user-defined map() block
            // that's called down below.
            final AbstractTouchMapEmitBlock emitBlock = new AbstractTouchMapEmitBlock() {

                @Override
                public void emitJSON(String keyJson, String valueJson, Long sequence) {
                    try {
                        //Log.v(Log.TAG_VIEW, "    emit(" + keyJson + ", "
                        //        + valueJson + ")");
                        ContentValues insertValues = new ContentValues();
                        insertValues.put("view_id", getViewId());
                        insertValues.put("sequence", sequence);
                        insertValues.put("key", keyJson);
                        insertValues.put("value", valueJson);
                        synchronized (inserts) {
                            inserts.add(insertValues);
                            added++;
                        }
                    } catch (Exception e) {
                        Log.e(Log.TAG_VIEW, "Error emitting", e);
                        // find a better way to propagate this back
                    }
                }

                @Override
                public void emitJSON(SpecialKey key, String valueJson, Long sequence) {
                    ContentValues ftsValues = new ContentValues();
                    ftsValues.put("content", key.getText());
                    long ftsId = database.getDatabase().insert("fulltext", null, ftsValues);

                    ContentValues insertValues = new ContentValues();
                    insertValues.put("view_id", getViewId());
                    insertValues.put("sequence", sequence);
                    insertValues.put("key", key.getText());
                    insertValues.put("value", valueJson);
                    insertValues.put("fulltext_id", ftsId);
                    synchronized (inserts) {
                        inserts.add(insertValues);
                        added++;
                    }
                }
            };

            boolean checkDocTypes = !getDocumentTypes().isEmpty() && !database.hasDataWithoutFpType();

            // Now scan every revision added since the last time the view was
            // indexed:
            String[] selectArgs = { Long.toString(lastSequence) };

            cursor = database.getDatabase().rawQuery("SELECT revs.doc_id, sequence, docid, revid, json, no_attachments "
                            + (checkDocTypes ? ", doc_type " : "")
                            + "FROM revs, docs "
                            + "WHERE sequence>? AND current!=0 AND deleted=0 "
                            + (checkDocTypes ? "AND doc_type IN (" + getJoinedSQLQuotedStrings(getDocumentTypes()) + ") " : "")
                            + "AND revs.doc_id = docs.doc_id "
                            + "ORDER BY revs.doc_id, revid DESC", selectArgs);


            boolean keepGoing = cursor.moveToNext();
            while (keepGoing) {
                // Get row values now, before the code below advances 'r':
                long docID = cursor.getLong(0);

                // Reconstitute the document as a dictionary:
                sequence = cursor.getLong(1);
                final String docId = cursor.getString(2);
                if(docId.startsWith("_design/")) {  // design docs don't get indexed!
                    keepGoing = cursor.moveToNext();
                    continue;
                }

                String revId = cursor.getString(3);
                byte[] json = cursor.getBlob(4);

                final boolean noAttachments = cursor.getInt(5) > 0;

                String docType = checkDocTypes ? cursor.getString(6) : null;

                final ArrayList<String> conflicts = new ArrayList<String>();
                while ((keepGoing = cursor.moveToNext()) &&  cursor.getLong(0) == docID) {
                    // Skip rows with the same doc_id -- these are losing conflicts.
                    final String conflictRev = cursor.getString(3);

                    if (conflictRev != null) conflicts.add(conflictRev);
                }

                if (lastSequence > 0) {
                    // Find conflicts with documents from previous indexings.
                    Boolean first = true;
                    String[] selectArgs2 = { Long.toString(docID), Long.toString(lastSequence) };

                    Cursor cursor2 = null;
                    try {
                        cursor2 = database.getDatabase().rawQuery(
                                "SELECT revid, sequence FROM revs "
                                        + "WHERE doc_id=? AND sequence<=? AND current!=0 AND deleted=0 "
                                        + "ORDER BY revID DESC "
                                        + "LIMIT 1", selectArgs2);

                        if (cursor2.moveToNext()) {
                            String oldRevId = cursor2.getString(0);
                            if (oldRevId != null) { conflicts.add(oldRevId); }

                            if (first) {
                                // This is the revision that used to be the 'winner'.
                                // Remove its emitted rows:
                                first = false;
                                long oldSequence = cursor2.getLong(1);
                                String[] args = {
                                        Integer.toString(getViewId()),
                                        Long.toString(oldSequence)
                                };
                                database.getDatabase().execSQL(
                                        "DELETE FROM maps WHERE view_id=? AND sequence=?", args);
                                if (RevisionInternal.CBLCompareRevIDs(oldRevId, revId) > 0) {
                                    // It still 'wins' the conflict, so it's the one that
                                    // should be mapped [again], not the current revision!
                                    conflicts.remove(oldRevId);
                                    conflicts.add(revId);
                                    revId = oldRevId;
                                    sequence = oldSequence;

                                    String[] selectArgs3 = {Long.toString(sequence)};
                                    json = Utils.byteArrayResultForQuery(database.getDatabase(), "SELECT json FROM revs WHERE sequence=?", selectArgs3);

                                }
                            }
                        }
                    } finally {
                        if (cursor2 != null) {
                            cursor2.close();
                        }
                    }

                    if (!first && !conflicts.isEmpty()) {
                        // Re-sort the conflict array if we added more revisions to it:
                        Collections.sort(conflicts, new Comparator<String>() {
                            @Override
                            public int compare(String rev1, String rev2) {
                                return RevisionInternal.CBLCompareRevIDs(rev1, rev2);
                            }
                        });
                    }
                }

                EnumSet<TDContentOptions> contentOptions = EnumSet.noneOf(Database.TDContentOptions.class);

                if (noAttachments || skipAttachments) {
                    contentOptions.add(TDContentOptions.TDNoAttachments);
                }

                RevisionInternal rev = new RevisionInternal(docId, revId, false, database);
                rev.setSequence(sequence);
                final  Map<String, Object> extra = database.extraPropertiesForRevision(rev, contentOptions);

                final byte[] finalJson = json;
                final long finalSequence = sequence;

                // skip; view's documentType doesn't match this doc
                if (checkDocTypes && !getDocumentTypes().contains(docType)) continue;

                taskExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        final Map<String, Object> properties = database.mapJsonToObject(
                                finalJson,
                                extra
                        );

                        if (properties != null) {
                            if (!conflicts.isEmpty()) {
                                // Add a "_conflicts" property if there were conflicting revisions:
                                properties.put("_conflicts", conflicts);
                            }

                            // Call the user-defined map() to emit new key/value
                            // pairs from this revision:
                            emitBlock.setSequence(finalSequence);

                            mapBlock.map(properties, emitBlock, finalSequence);
                        }

                    }
                });

            }

            //XXX implement bulk doc processing (put some data in 30 seconds, return and then keep adding in background until fully done).
            // + if it will be implemented -> think about the case when view is changed and we need to stop doing background data processing.
            boolean tasksFinishedInTime = false;
            try {
                taskExecutor.shutdown();
                tasksFinishedInTime = taskExecutor.awaitTermination(30, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                tasksFinishedInTime = false;
                Log.w(Log.TAG_VIEW, "Unable to pre-calculate all views. Task is cancelled or executor is shutdown", e);
            }


            for (ContentValues contentValues : inserts) {
                database.getDatabase().insert("maps", null, contentValues);
            }

            // Finally, record the last revision sequence number that was
            // indexed:
            ContentValues updateValues = new ContentValues();
            updateValues.put("lastSequence", dbMaxSequence);
            updateValues.put("total_docs", countTotalRows());
            String[] whereArgs = { Integer.toString(getViewId()) };
            database.getDatabase().update("views", updateValues, "view_id=?",
                    whereArgs);

            Date d2 = new Date();
            Log.v(Log.TAG_VIEW, "Finished re-indexing view: %s "
                    + " up to sequence %s"
                    + " (deleted %s added %s) in %s ms", name, dbMaxSequence, deleted, added, (d2.getTime() - d1.getTime()));
            result.setCode(Status.OK);

        } catch (SQLException e) {
            throw new CouchbaseLiteException(e, new Status(Status.DB_ERROR));
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            if (!result.isSuccessful()) {
                Log.w(Log.TAG_VIEW, "Failed to rebuild view %s.  Result code: %d", name, result.getCode());
            }
            if(database != null) {
                database.endTransaction(result.isSuccessful());
            }
        }

    }

    private static String getJoinedSQLQuotedStrings(Collection<String> strings) {
        if (strings == null || strings.isEmpty()) return "";

        StringBuilder sb = new StringBuilder("'");
        boolean first = true;
        for (String s : strings) {
            if (first)
                first = false;
            else
                sb.append("','");
            sb.append(s.replace("'", "''"));
        }
        sb.append('\'');
        return sb.toString();
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Cursor resultSetWithOptions(QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }

        // OPT: It would be faster to use separate tables for raw-or ascii-collated views so that
        // they could be indexed with the right collation, instead of having to specify it here.
        String collationStr = "";
        if(collation == TDViewCollation.TDViewCollationASCII) {
            collationStr += " COLLATE JSON_ASCII";
        }
        else if(collation == TDViewCollation.TDViewCollationRaw) {
            collationStr += " COLLATE JSON_RAW";
        }

        String sql = "SELECT key, value, docid, revs.sequence";
        if (options.isIncludeDocs()) {
            sql = sql + ", revid, json";
        }
        sql = sql + " FROM maps, revs, docs WHERE maps.view_id=?";

        List<String> argsList = new ArrayList<String>();
        argsList.add(Integer.toString(getViewId()));

        if(options.getKeys() != null) {
            sql += " AND key in (";
            String item = "?";
            for (Object key : options.getKeys()) {
                sql += item;
                item = ", ?";
                argsList.add(toJSONString(key));
            }
            sql += ")";
        }

        String startKey = toJSONString(options.getStartKey());
        String endKey = toJSONString(options.getEndKey());
        String minKey = startKey;
        String maxKey = endKey;
        String minKeyDocId = options.getStartKeyDocId();
        String maxKeyDocId = options.getEndKeyDocId();

        boolean inclusiveMin = true;
        boolean inclusiveMax = options.isInclusiveEnd();
        if (options.isDescending()) {
            String min = minKey;
            minKey = maxKey;
            maxKey = min;
            inclusiveMin = inclusiveMax;
            inclusiveMax = true;
            minKeyDocId = options.getEndKeyDocId();
            maxKeyDocId = options.getStartKeyDocId();
        }

        if (minKey != null) {
            if (inclusiveMin) {
                sql += " AND key >= ?";
            } else {
                sql += " AND key > ?";
            }
            sql += collationStr;
            argsList.add(minKey);
            if (minKeyDocId != null && inclusiveMin) {
                //OPT: This calls the JSON collator a 2nd time unnecessarily.
                sql += String.format(" AND (key > ? %s OR docid >= ?)", collationStr);
                argsList.add(minKey);
                argsList.add(minKeyDocId);
            }
        }

        if (maxKey != null) {
            if (inclusiveMax) {
                sql += " AND key <= ?";
            } else {
                sql += " AND key < ?";
            }
            sql += collationStr;
            argsList.add(maxKey);
            if (maxKeyDocId != null && inclusiveMax) {
                sql += String.format(" AND (key < ? %s OR docid <= ?)", collationStr);
                argsList.add(maxKey);
                argsList.add(maxKeyDocId);
            }
        }

        sql = sql
                + " AND revs.sequence = maps.sequence AND docs.doc_id = revs.doc_id ORDER BY key";
        sql += collationStr;

        if (options.isDescending()) {
            sql = sql + " DESC";
        }

        sql = sql + " LIMIT ? OFFSET ?";
        argsList.add(Integer.toString(options.getLimit()));
        argsList.add(Integer.toString(options.getSkip()));

        Log.v(Log.TAG_VIEW, "Query %s: %s | args: %s", name, sql, argsList);

        Cursor cursor = database.getDatabase().rawQuery(sql,
                argsList.toArray(new String[argsList.size()]));
        return cursor;
    }

    /**
     * Are key1 and key2 grouped together at this groupLevel?
     * @exclude
     */
    @InterfaceAudience.Private
    public static boolean groupTogether(Object key1, Object key2, int groupLevel) {
        if(groupLevel == 0 || !(key1 instanceof List) || !(key2 instanceof List)) {
            return key1.equals(key2);
        }
        @SuppressWarnings("unchecked")
        List<Object> key1List = (List<Object>)key1;
        @SuppressWarnings("unchecked")
        List<Object> key2List = (List<Object>)key2;

        // if either key list is smaller than groupLevel and the key lists are different
        // sizes, they cannot be equal.
        if ((key1List.size() < groupLevel || key2List.size() < groupLevel) && key1List.size() != key2List.size()) {
            return false;
        }

        int end = Math.min(groupLevel, Math.min(key1List.size(), key2List.size()));
        for(int i = 0; i < end; ++i) {
            if(!key1List.get(i).equals(key2List.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the prefix of the key to use in the result row, at this groupLevel
     * @exclude
     */
    @SuppressWarnings("unchecked")
    @InterfaceAudience.Private
    public static Object groupKey(Object key, int groupLevel) {
        if(groupLevel > 0 && (key instanceof List) && (((List<Object>)key).size() > groupLevel)) {
            return ((List<Object>)key).subList(0, groupLevel);
        }
        else {
            return key;
        }
    }

    /*** Querying ***/

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public List<Map<String, Object>> dump() {
        if (getViewId() < 0) {
            return null;
        }

        String[] selectArgs = { Integer.toString(getViewId()) };
        Cursor cursor = null;
        List<Map<String, Object>> result = null;

        try {
            cursor = database
                    .getDatabase()
                    .rawQuery(
                            "SELECT sequence, key, value FROM maps WHERE view_id=? ORDER BY key",
                            selectArgs);

            cursor.moveToNext();
            result = new ArrayList<Map<String, Object>>();
            while (!cursor.isAfterLast()) {
                Map<String, Object> row = new HashMap<String, Object>();
                row.put("seq", cursor.getInt(0));
                row.put("key", cursor.getString(1));
                row.put("value", cursor.getString(2));
                result.add(row);
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            Log.e(Log.TAG_VIEW, "Error dumping view", e);
            return null;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    List<QueryRow> reducedQuery(Cursor cursor, boolean group, int groupLevel) throws CouchbaseLiteException {

        List<Object> keysToReduce = null;
        List<Object> valuesToReduce = null;
        Object lastKey = null;
        if(getReduce() != null) {
            keysToReduce = new ArrayList<Object>(REDUCE_BATCH_SIZE);
            valuesToReduce = new ArrayList<Object>(REDUCE_BATCH_SIZE);
        }
        List<QueryRow> rows = new ArrayList<QueryRow>();

        cursor.moveToNext();
        while (!cursor.isAfterLast()) {
            JsonDocument keyDoc = new JsonDocument(cursor.getBlob(0));
            JsonDocument valueDoc = new JsonDocument(cursor.getBlob(1));
            assert(keyDoc != null);

            Object keyObject = keyDoc.jsonObject();
            if(group && !groupTogether(keyObject, lastKey, groupLevel)) {
                if (lastKey != null) {
                    // This pair starts a new group, so reduce & record the last one:
                    Object reduced = (reduceBlock != null) ? reduceBlock.reduce(keysToReduce, valuesToReduce, false) : null;
                    Object key = groupKey(lastKey, groupLevel);
                    QueryRow row = new QueryRow(null, 0, key, reduced, null);
                    row.setDatabase(database);
                    rows.add(row);
                    keysToReduce.clear();
                    valuesToReduce.clear();

                }
                lastKey = keyObject;
            }
            keysToReduce.add(keyObject);
            valuesToReduce.add(valueDoc.jsonObject());

            cursor.moveToNext();

        }

        if(keysToReduce.size() > 0) {
            // Finish the last group (or the entire list, if no grouping):
            Object key = group ? groupKey(lastKey, groupLevel) : null;
            Object reduced = (reduceBlock != null) ? reduceBlock.reduce(keysToReduce, valuesToReduce, false) : null;
            QueryRow row = new QueryRow(null, 0, key, reduced, null);
            row.setDatabase(database);
            rows.add(row);
        }

        return rows;

    }

    /**
     * Queries the view. Does NOT first update the index.
     *
     * @param options The options to use.
     * @return An array of QueryRow objects.
     * @exclude
     */
    @InterfaceAudience.Private
    public List<QueryRow> queryWithOptions(QueryOptions options) throws CouchbaseLiteException {

        Date d1 = new Date();
        if (options == null) {
            options = new QueryOptions();
        }

        if (options.getFullTextQuery() != null) {
            return queryFullText(options);
        }

        Cursor cursor = null;
        List<QueryRow> rows = new ArrayList<QueryRow>();

        try {
            cursor = resultSetWithOptions(options);
            int groupLevel = options.getGroupLevel();
            boolean group = options.isGroup() || (groupLevel > 0);
            boolean reduce = options.isReduce() || group;

            if (reduce && (reduceBlock == null) && !group) {
                Log.w(Log.TAG_VIEW, "Cannot use reduce option in view %s which has no reduce block defined", name);
                throw new CouchbaseLiteException(new Status(Status.BAD_REQUEST));
            }

            if (reduce || group) {
                // Reduced or grouped query:
                rows = reducedQuery(cursor, group, groupLevel);
            } else {
                // regular query
                cursor.moveToNext();
                while (!cursor.isAfterLast()) {
                    JsonDocument keyDoc = new JsonDocument(cursor.getBlob(0));
                    JsonDocument valueDoc = new JsonDocument(cursor.getBlob(1));
                    String docId = cursor.getString(2);
                    int sequence =  Integer.valueOf(cursor.getString(3));
                    Map<String, Object> docContents = null;

                    if (options.isIncludeDocs()) {
                        Object valueObject = valueDoc.jsonObject();

                        try { // http://wiki.apache.org/couchdb/Introduction_to_CouchDB_views#Linked_documents
                            if (valueObject instanceof Map) {
                                String linkedDocId = (String) ((Map) valueObject).get("_id");
                                String linkedRevId = null;
                                if (((Map) valueObject).get("_rev") != null) linkedRevId = (String) ((Map) valueObject).get("_rev");
                                RevisionInternal linkedDoc = database.getDocumentWithIDAndRev(linkedDocId, linkedRevId, EnumSet.noneOf(TDContentOptions.class));

                                if (linkedDoc == null) docContents = null;
                                else docContents = linkedDoc.getProperties();
                            } else {
                                docContents = database.documentPropertiesFromJSON(
                                    cursor.getBlob(5),
                                    docId,
                                    cursor.getString(4),
                                    false,
                                    cursor.getLong(3),
                                    options.getContentOptions()
                                );    
                            }
                        } catch (Exception e) {
                            docContents = database.documentPropertiesFromJSON(
                                    cursor.getBlob(5),
                                    docId,
                                    cursor.getString(4),
                                    false,
                                    cursor.getLong(3),
                                    options.getContentOptions()
                            );
                        }
                    }

                    QueryRow row = new QueryRow(docId, sequence, keyDoc.jsonObject(), valueDoc.jsonObject(), docContents);
                    row.setDatabase(database);
                    rows.add(row);
                    cursor.moveToNext();
                }
            }

        } catch (SQLException e) {
            String errMsg = String.format("Error querying view: %s", this);
            Log.e(Log.TAG_VIEW, errMsg, e);
            throw new CouchbaseLiteException(errMsg, e, new Status(Status.DB_ERROR));
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        Date d2 = new Date();
        Log.v(Log.TAG_VIEW, "Query: %s: Returning %s rows, took %s ms", name, rows.size(), (d2.getTime()-d1.getTime()));
        return rows;

    }

    public List<QueryRow> queryFullText(QueryOptions options) throws CouchbaseLiteException {
//        String sql = "SELECT docs.docid, maps.sequence, maps.fulltext_id, maps.value, offsets(fulltext)";
        String sql = "SELECT docs.docid, maps.sequence, maps.fulltext_id, maps.value, offsets(fulltext)";

//        if (options->fullTextSnippets)
//        [sql appendString: @", snippet(fulltext, '\001','\002','…')"];

        sql += " FROM maps, fulltext, revs, docs "
                + "WHERE fulltext.content MATCH ? AND maps.fulltext_id = fulltext.rowid "
                + "AND maps.view_id = ? "
                + "AND revs.sequence = maps.sequence AND docs.doc_id = revs.doc_id ";

//        if (options->fullTextRanking)
//        [sql appendString: @"ORDER BY - ftsrank(matchinfo(fulltext)) "];
//        else
//        [sql appendString: @"ORDER BY maps.sequence "];
        sql += "ORDER BY maps.sequence ";


        if (options.isDescending()) {
            sql += " DESC";
        }
        sql += " LIMIT ? OFFSET ?";

        int limit = options.getLimit();

        List<QueryRow> rows = new ArrayList<QueryRow>();
        Cursor cursor = null;
        long result = -1;
        try {
            String[] args = { options.getFullTextQuery(), Integer.toString(getViewId()), Integer.toString(limit), Integer.toString(options.getSkip()) };
            cursor = database.getDatabase().rawQuery(sql, args);
            cursor.moveToNext();
            while (!cursor.isAfterLast()) {
                String docId = cursor.getString(0);
                long sequence =  Long.valueOf(cursor.getString(1));
                int fullTextId =  Integer.valueOf(cursor.getString(2));
                String offsets = cursor.getString(4);
                JsonDocument valueDoc = new JsonDocument(cursor.getBlob(3));

                FullTextQueryRow row = new FullTextQueryRow(docId, sequence, fullTextId, offsets, valueDoc.jsonObject());
                row.setDatabase(database);
                rows.add(row);
                cursor.moveToNext();

            }
        } catch (Exception e) {
            Log.e(Log.TAG_VIEW, "Error getting last sequence indexed", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            return rows;
        }
    }

    /**
     * Utility function to use in reduce blocks. Totals an array of Numbers.
     * @exclude
     */
    @InterfaceAudience.Private
    public static double totalValues(List<Object>values) {
        double total = 0;
        for (Object object : values) {
            if(object instanceof Number) {
                Number number = (Number)object;
                total += number.doubleValue();
            } else {
                Log.w(Log.TAG_VIEW, "Warning non-numeric value found in totalValues: %s", object);
            }
        }
        return total;
    }

}

@InterfaceAudience.Private
abstract class AbstractTouchMapEmitBlock implements Emitter {

    protected long sequence = 0;

    void setSequence(long sequence) {
        this.sequence = sequence;
    }

}
