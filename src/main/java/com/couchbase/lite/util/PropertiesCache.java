package com.couchbase.lite.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by paolo on 29/07/16.
 */
public class PropertiesCache extends ConcurrentHashMap<String, Map> {

    private final static String TAG = PropertiesCache.class.getSimpleName();

    private final static int QUOTA_OF_MEMORY_RESERVED_TO_CACHING = 4;
    private final static float LOAD_FACTOR = 0.9f; //the load factor threshold, used to control resizing. Resizing may be performed when the average number of elements per bin exceeds this threshold.
    private final static int CONCURRENCY_LEVEL = Runtime.getRuntime().availableProcessors(); //the estimated number of concurrently updating threads. The implementation performs internal sizing to try to accommodate this many threads.

    private static int initialCapacity;
    private static ConcurrentHashMap<String, Map> instance;


    private PropertiesCache() {
        long memoryAvailable = getPresumableMemoryAvailable();
        calculateInitialCapacity(memoryAvailable);
        instance = new ConcurrentHashMap<>(initialCapacity,LOAD_FACTOR,CONCURRENCY_LEVEL);
        Log.e(TAG,"create new PropertiesCache object");
    }

    public static ConcurrentHashMap getInstance() {
        if (instance == null) {
            instance = new PropertiesCache();
        }
        return instance;
    }

    private static long getPresumableMemoryAvailable() {
        long allocatedMemory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
        long presumableFreeMemory = Runtime.getRuntime().maxMemory() - allocatedMemory;
        Log.e(TAG,"presumableFreeMemory: " + presumableFreeMemory);
        return presumableFreeMemory;
    }

    private static void calculateInitialCapacity(long memoryAvailable) {
        long memoryAvailabeInMb = memoryAvailable / 1048576;
        initialCapacity = (int)(memoryAvailabeInMb / QUOTA_OF_MEMORY_RESERVED_TO_CACHING) * 100;
        Log.e(TAG,"initialCapacity for this cache is: " + initialCapacity);
    }

    public static boolean addPropertyIfCacheIsNotFull(String key, Map property) {
        assert (key != null);
        assert (property != null);
        if (instance == null) {
            getInstance();
        }
        if (instance.size() < initialCapacity) {
            instance.put(key,property);
            return true;
        }
        return false;
    }



    public static Map getProperty(String key){
        assert (key != null);
        if (instance == null) {
            getInstance();
        }
        return instance.get(key);

    }

    public static void clearAll() {
        instance.clear();
    }

    public static int cacheSize() {
        return instance.size();
    }

    public static int maxSize() {
        return initialCapacity;
    }

}
