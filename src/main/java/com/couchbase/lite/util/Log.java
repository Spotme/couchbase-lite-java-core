/**
 * Created by Wayne Carter.
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

package com.couchbase.lite.util;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Log {

    private static Logger logger;
    private static CountDownLatch initLogLatch;

    /**
     * A map of tags and their enabled log level
     */
    private static ConcurrentHashMap<String, Integer> enabledTags;

    /**
     * Logging tags
     */
    public static final String TAG = "CBLite";  // default "catch-all" tag
    public static final String TAG_SYNC = "Sync";
    public static final String TAG_BATCHER = "Batcher";
    public static final String TAG_SYNC_ASYNC_TASK = "SyncAsyncTask";
    public static final String TAG_REMOTE_REQUEST = "RemoteRequest";
    public static final String TAG_VIEW = "View";
    public static final String TAG_QUERY = "Query";
    public static final String TAG_CHANGE_TRACKER = "ChangeTracker";
    public static final String TAG_ROUTER = "Router";
    public static final String TAG_DATABASE = "Database";
    public static final String TAG_LISTENER = "Listener";
    public static final String TAG_MULTI_STREAM_WRITER = "MultistreamWriter";
    public static final String TAG_BLOB_STORE = "BlobStore";
    public static final String TAG_SYMMETRIC_KEY = "SymmetricKey";
    public static final String TAG_ACTION = "Action";


    /**
     * Logging levels -- values match up with android.util.Log
     */
    public static final int VERBOSE = 2;
    public static final int DEBUG = 3;
    public static final int INFO = 4;
    public static final int WARN = 5;
    public static final int ERROR = 6;
    public static final int ASSERT = 7;

    static {
        enabledTags = new ConcurrentHashMap<String, Integer>();
        enableAllWarnLogs();

        //lazy init logger
        new Thread() {
            @Override
            public void run() {
                createLoggerBlockingThread();
            }
        }.start();
    }

    public static void enableAllWarnLogs() {
        enabledTags.put(Log.TAG, WARN);
        enabledTags.put(Log.TAG_SYNC, WARN);
        enabledTags.put(Log.TAG_SYNC_ASYNC_TASK, WARN);
        enabledTags.put(Log.TAG_REMOTE_REQUEST, WARN);
        enabledTags.put(Log.TAG_VIEW, WARN);
        enabledTags.put(Log.TAG_QUERY, WARN);
        enabledTags.put(Log.TAG_CHANGE_TRACKER, WARN);
        enabledTags.put(Log.TAG_ROUTER, WARN);
        enabledTags.put(Log.TAG_DATABASE, WARN);
        enabledTags.put(Log.TAG_LISTENER, WARN);
        enabledTags.put(Log.TAG_MULTI_STREAM_WRITER, WARN);
        enabledTags.put(Log.TAG_BLOB_STORE, WARN);
        enabledTags.put(Log.TAG_SYMMETRIC_KEY, WARN);
        enabledTags.put(Log.TAG_ACTION, WARN);
        enabledTags.put(Log.TAG_BATCHER, WARN);
    }

    public static void disableAllLogs() {
        enabledTags.put(Log.TAG, ASSERT);
        enabledTags.put(Log.TAG_SYNC, ASSERT);
        enabledTags.put(Log.TAG_SYNC_ASYNC_TASK, ASSERT);
        enabledTags.put(Log.TAG_REMOTE_REQUEST, ASSERT);
        enabledTags.put(Log.TAG_VIEW, ASSERT);
        enabledTags.put(Log.TAG_QUERY, ASSERT);
        enabledTags.put(Log.TAG_CHANGE_TRACKER, ASSERT);
        enabledTags.put(Log.TAG_ROUTER, ASSERT);
        enabledTags.put(Log.TAG_DATABASE, ASSERT);
        enabledTags.put(Log.TAG_LISTENER, ASSERT);
        enabledTags.put(Log.TAG_MULTI_STREAM_WRITER, ASSERT);
        enabledTags.put(Log.TAG_BLOB_STORE, ASSERT);
        enabledTags.put(Log.TAG_SYMMETRIC_KEY, ASSERT);
        enabledTags.put(Log.TAG_ACTION, ASSERT);
        enabledTags.put(Log.TAG_BATCHER, ASSERT);
    }

    /**
     * Enable logging for a particular tag / loglevel combo
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param logLevel The loglevel to enable.  Anything matching this loglevel
     *                 or having a more urgent loglevel will be emitted.  Eg, Log.VERBOSE.
     */
    public static void enableLogging(String tag, int logLevel) {
        enabledTags.put(tag, logLevel);
    }

    /**
     * Is logging enabled for given tag / loglevel combo?
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param logLevel The loglevel to check whether it's enabled.  Will match this loglevel
     *                 or a more urgent loglevel.  Eg, if Log.ERROR is enabled and Log.VERBOSE
     *                 is passed as a paremeter, it will return true.
     * @return boolean indicating whether logging is enabled.
     */
    /* package */ static boolean isLoggingEnabled(String tag, int logLevel) {

        // this hashmap lookup might be a little expensive, and so it might make
        // sense to convert this over to a CopyOnWriteArrayList
        Integer logLevelForTag = enabledTags.get(tag);
        return logLevel >= (logLevelForTag == null ? INFO : logLevelForTag);
    }

    /**
     * Send a VERBOSE message.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param msg The message you would like logged.
     */
    public static void v(String tag, String msg) {
        if (getLogger() != null && isLoggingEnabled(tag, VERBOSE)) {
            getLogger().v(tag, msg);
        }
    }

    /**
     * Send a VERBOSE message and log the exception.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param msg The message you would like logged.
     * @param tr An exception to log
     */
    public static void v(String tag, String msg, Throwable tr) {
        if (getLogger() != null && isLoggingEnabled(tag, VERBOSE)) {
            getLogger().v(tag, msg, tr);
        }
    }

    /**
     * Send a VERBOSE message.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param formatString The string you would like logged plus format specifiers.
     * @param args Variable number of Object args to be used as params to formatString.
     */
    public static void v(String tag, String formatString, Object... args) {
        if (getLogger() != null && isLoggingEnabled(tag, VERBOSE)) {
            try {
                getLogger().v(tag, String.format(Locale.ENGLISH, formatString, args));
            } catch (Exception e) {
                getLogger().v(tag, String.format(Locale.ENGLISH, "Unable to format log: %s", formatString), e);
            }
        }

    }

    /**
     * Send a VERBOSE message and log the exception.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param formatString The string you would like logged plus format specifiers.
     * @param tr An exception to log
     * @param args Variable number of Object args to be used as params to formatString.
     */
    public static void v(String tag, String formatString, Throwable tr, Object... args) {
        if (getLogger() != null && isLoggingEnabled(tag, VERBOSE)) {
            try {
                getLogger().v(tag, String.format(Locale.ENGLISH, formatString, args), tr);
            } catch (Exception e) {
                getLogger().v(tag, String.format(Locale.ENGLISH, "Unable to format log: %s", formatString), e);
            }
        }
    }

    /**
     * Send a DEBUG message.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param msg The message you would like logged.
     */
    public static void d(String tag, String msg) {
        if (getLogger() != null && isLoggingEnabled(tag, DEBUG)) {
            getLogger().d(tag, msg);
        }
    }

    /**
     * Send a DEBUG message and log the exception.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param msg The message you would like logged.
     * @param tr An exception to log
     */
    public static void d(String tag, String msg, Throwable tr) {
        if (getLogger() != null && isLoggingEnabled(tag, DEBUG)) {
            getLogger().d(tag, msg, tr);
        }
    }

    /**
     * Send a DEBUG message.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param formatString The string you would like logged plus format specifiers.
     * @param args Variable number of Object args to be used as params to formatString.
     */
    public static void d(String tag, String formatString, Object... args) {
        if (getLogger() != null && isLoggingEnabled(tag, DEBUG)) {
            try {
                getLogger().d(tag, String.format(Locale.ENGLISH, formatString, args));
            } catch (Exception e) {
                getLogger().d(tag, String.format(Locale.ENGLISH, "Unable to format log: %s", formatString), e);
            }
        }
    }

    /**
     * Send a DEBUG message and log the exception.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param formatString The string you would like logged plus format specifiers.
     * @param tr An exception to log
     * @param args Variable number of Object args to be used as params to formatString.
     */
    public static void d(String tag, String formatString, Throwable tr, Object... args) {
        if (getLogger() != null && isLoggingEnabled(tag, DEBUG)) {
            try {
                getLogger().d(tag, String.format(Locale.ENGLISH, formatString, args, tr));
            } catch (Exception e) {
                getLogger().d(tag, String.format(Locale.ENGLISH, "Unable to format log: %s", formatString), e);
            }
        }
    }


    /**
     * Send an INFO message.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param msg The message you would like logged.
     */
    public static void i(String tag, String msg) {
        if (getLogger() != null && isLoggingEnabled(tag, INFO)) {
            getLogger().i(tag, msg);
        }
    }

    /**
     * Send a INFO message and log the exception.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param msg The message you would like logged.
     * @param tr An exception to log
     */
    public static void i(String tag, String msg, Throwable tr) {
        if (getLogger() != null && isLoggingEnabled(tag, INFO)) {
            getLogger().i(tag, msg, tr);
        }
    }

    /**
     * Send an INFO message.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param formatString The string you would like logged plus format specifiers.
     * @param args Variable number of Object args to be used as params to formatString.
     */
    public static void i(String tag, String formatString, Object... args) {
        if (getLogger() != null && isLoggingEnabled(tag, INFO)) {
            try {
                getLogger().i(tag, String.format(Locale.ENGLISH, formatString, args));
            } catch (Exception e) {
                getLogger().i(tag, String.format(Locale.ENGLISH, "Unable to format log: %s", formatString), e);
            }
        }
    }

    /**
     * Send a INFO message and log the exception.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param formatString The string you would like logged plus format specifiers.
     * @param tr An exception to log
     * @param args Variable number of Object args to be used as params to formatString.
     */
    public static void i(String tag, String formatString, Throwable tr, Object... args) {
        if (getLogger() != null && isLoggingEnabled(tag, INFO)) {
            try {
                getLogger().i(tag, String.format(Locale.ENGLISH, formatString, args, tr));
            } catch (Exception e) {
                getLogger().i(tag, String.format(Locale.ENGLISH, "Unable to format log: %s", formatString), e);
            }
        }
    }

    /**
     * Send a WARN message.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param msg The message you would like logged.
     */
    public static void w(String tag, String msg) {
        if (getLogger() != null && isLoggingEnabled(tag, WARN)) {
            getLogger().w(tag, msg);
        }
    }

    /**
     * Send a WARN message and log the exception.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param tr An exception to log
     */
    public static void w(String tag, Throwable tr) {
        if (getLogger() != null && isLoggingEnabled(tag, WARN)) {
            getLogger().w(tag, tr);
        }
    }

    /**
     * Send a WARN message and log the exception.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param msg The message you would like logged.
     * @param tr An exception to log
     */
    public static void w(String tag, String msg, Throwable tr) {
        if (getLogger() != null && isLoggingEnabled(tag, WARN)) {
            getLogger().w(tag, msg, tr);
        }
    }


    /**
     * Send a WARN message.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param formatString The string you would like logged plus format specifiers.
     * @param args Variable number of Object args to be used as params to formatString.
     */
    public static void w(String tag, String formatString, Object... args) {
        if (getLogger() != null && isLoggingEnabled(tag, WARN)) {
            try {
                getLogger().w(tag, String.format(Locale.ENGLISH, formatString, args));
            } catch (Exception e) {
                getLogger().w(tag, String.format(Locale.ENGLISH, "Unable to format log: %s", formatString), e);
            }
        }
    }


    /**
     * Send a WARN message and log the exception.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param formatString The string you would like logged plus format specifiers.
     * @param tr An exception to log
     * @param args Variable number of Object args to be used as params to formatString.
     */
    public static void w(String tag, String formatString, Throwable tr, Object... args) {
        if (getLogger() != null && isLoggingEnabled(tag, WARN)) {
            try {
                getLogger().w(tag, String.format(Locale.ENGLISH, formatString, args), tr);
            } catch (Exception e) {
                getLogger().w(tag, String.format(Locale.ENGLISH, "Unable to format log: %s", formatString), e);
            }
        }
    }


    /**
     * Send an ERROR message.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param msg The message you would like logged.
     */
    public static void e(String tag, String msg) {
        if (getLogger() != null && isLoggingEnabled(tag, ERROR)) {
            getLogger().e(tag, msg);
        }
    }

    /**
     * Send a ERROR message and log the exception.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param msg The message you would like logged.
     * @param tr An exception to log
     */
    public static void e(String tag, String msg, Throwable tr) {
        if (getLogger() != null && isLoggingEnabled(tag, ERROR)) {
            getLogger().e(tag, msg, tr);
        }
    }


    /**
     * Send a ERROR message and log the exception.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param formatString The string you would like logged plus format specifiers.
     * @param tr An exception to log
     * @param args Variable number of Object args to be used as params to formatString.
     */
    public static void e(String tag, String formatString, Throwable tr, Object... args) {
        if (getLogger() != null && isLoggingEnabled(tag, ERROR)) {
            try {
                getLogger().e(tag, String.format(Locale.ENGLISH, formatString, args), tr);
            } catch (Exception e) {
                getLogger().e(tag, String.format(Locale.ENGLISH, "Unable to format log: %s", formatString), e);
            }
        }
    }

    /**
     * Send a ERROR message.
     * @param tag Used to identify the source of a log message.  It usually identifies
     *        the class or activity where the log call occurs.
     * @param formatString The string you would like logged plus format specifiers.
     * @param args Variable number of Object args to be used as params to formatString.
     */
    public static void e(String tag, String formatString, Object... args) {
        if (getLogger() != null && isLoggingEnabled(tag, ERROR)) {
            try {
                getLogger().e(tag, String.format(Locale.ENGLISH, formatString, args));
            } catch (Exception e) {
                getLogger().e(tag, String.format(Locale.ENGLISH, "Unable to format log: %s", formatString), e);
            }
        }
    }


    public static Logger getLogger() {
        if (logger == null) {
            createLoggerBlockingThread();
        }
        return logger;
    }

    /**
     * Reads loager with Clasloader and init it.
     *
     *  Blocking current thread for reading & loading Logger class.
     */
    private static void createLoggerBlockingThread() {
        if (initLogLatch == null) {
            initLogLatch = new CountDownLatch(1);

            new Thread() {
                @Override
                public void run() {
                    logger = LoggerFactory.createLogger();

                    initLogLatch.countDown();
                }
            }.start();
        }

        try {
            final boolean completed = initLogLatch.await(1, TimeUnit.SECONDS);
            if (!completed) throw new TimeoutException("Unable to init log in 1 sec");
        } catch (InterruptedException | TimeoutException e) {
            e.printStackTrace();

            logger = new NullStateLogger();
        }
    }

    public static void setLogger(Logger logger) {
        Log.logger = logger;
    }

    //todo: change to System.out
    public static class NullStateLogger implements Logger {

        @Override
        public void v(String tag, String msg) {
        }

        @Override
        public void v(String tag, String msg, Throwable tr) {
        }

        @Override
        public void d(String tag, String msg) {
        }

        @Override
        public void d(String tag, String msg, Throwable tr) {
        }

        @Override
        public void i(String tag, String msg) {
        }

        @Override
        public void i(String tag, String msg, Throwable tr) {
        }

        @Override
        public void w(String tag, String msg) {
        }

        @Override
        public void w(String tag, Throwable tr) {
        }

        @Override
        public void w(String tag, String msg, Throwable tr) {
        }

        @Override
        public void e(String tag, String msg) {
        }

        @Override
        public void e(String tag, String msg, Throwable tr) {
        }
    }
}