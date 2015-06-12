package com.couchbase.lite.appscripts;

import sun.org.mozilla.javascript.internal.Undefined;

/**
 * Call-back, that is called when corespondent call-back function is called in JS script.
 * Usually corespondent call-back function is named done() and is last param in JS script function.
 */
public abstract class OnScriptExecutedCallBack {
    final ThreadToRunCallBack threadToRunCallBack;

    public OnScriptExecutedCallBack(ThreadToRunCallBack threadToRunCallBack) {
        this.threadToRunCallBack = threadToRunCallBack;
    }

    public OnScriptExecutedCallBack() {
        threadToRunCallBack = ThreadToRunCallBack.DEFAULT;
    }

    /**
     * Called when "done()" is called in JS script. Normally JS should do nothing after it.
     *
     * Implementers should consider using {@link #onErrorResult(Object)} and {@link #onSuccessResult(Object)} instead.
     * Default implementation dispatches incoming data to these methods.
     *
     * This call-back should be @Overridden only when Implementer do not care about result of "done()",
     * and just need to know, that execution of "App Script" is finished.
     *
     * @param callBackData data passed to "done()" bu JS script.
     */
    public final void onDone(Object[] callBackData) {
        if (callBackData.length > 0 && callBackData[0] != Undefined.instance) {
            final Object error = callBackData[0];
            onErrorResult(error);
        } else {
            final Object respObj;
            if (callBackData.length > 1 && callBackData[1] != Undefined.instance) {
                respObj = callBackData[1];
            } else {
                respObj = null;
            }
            onSuccessResult(respObj);
        }
    }

    /**
     * Called when "done()", that is called in JS script, contains NO errors. Normally JS should do nothing after done().
     *
     *
     * @param resultObj The result of the script execution, passed to "done()" in JS script. Undefined is represented as null for simplicity.
     *
     * @see #onErrorResult(java.lang.Object)
     * @see #onDone
     */
    protected abstract void onSuccessResult(Object resultObj);

    /**
     * Called when "done()", that is called in JS script, contains errors. Normally JS should do nothing after done().
     *
     * @param errorObj error passed to "done()" by JS script
     */
    protected abstract void onErrorResult(Object errorObj);

    /**
     * @return callers should call {@link #onDone(Object[]) method} on thread, returned by this method.
     */
    public ThreadToRunCallBack getThreadToRunCallBack() {
        return threadToRunCallBack;
    }

    static public enum ThreadToRunCallBack {
        DEFAULT, //thread, where JS called "done()" call-back. Either "event loop" or one of "AppScriptsWorkers" threads
        UI
    }
}
