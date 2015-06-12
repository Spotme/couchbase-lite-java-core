package com.couchbase.lite.appscripts;

/**
 * Call-back, that is called when corespondent call-back function is called in JS script.
 * Usually corespondent call-back function is named done() and is last param in JS script function.
 */
public abstract class OnScriptExecutedCallBack{

    /**
     * Called when "done()" is called in JS script. Normally JS should do nothing after it.
     *
     * @param error error if any
     * @param result result if any
     */
    public abstract void onDone(Object error, Object result);
}
