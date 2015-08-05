package com.couchbase.lite.appscripts;

import org.mozilla.javascript.RhinoException;
import org.mozilla.javascript.Undefined;

/**
 * Call-back, that is called when corespondent call-back function is called in JS script.
 * Usually corespondent call-back function is named done() and is last param in JS script function.
 */
public abstract class OnScriptExecutedCallBack {
    final ThreadToRun threadToRun;

    public OnScriptExecutedCallBack(ThreadToRun threadToRun) {
        this.threadToRun = threadToRun;
    }

    public OnScriptExecutedCallBack() {
        threadToRun = ThreadToRun.DEFAULT;
    }

    /**
     * Called when "done()" is called in JS script. Normally JS should do nothing after it.
     *
     * Implementers should consider using {@link #onErrorResult(Throwable)} and {@link #onSuccessResult(Object)} instead.
     * Default implementation dispatches incoming data to these methods.
     *
     * This call-back should be @Overridden only when Implementer do not care about result of "done()",
     * and just need to know, that execution of "App Script" is finished.
     *
     * Must be called from the thread specified with {@link #getThreadToRun()}.
     *
     * @param callBackData data passed to "done()" bu JS script.
     */
    public final void onDone(Object[] callBackData) {
        if (callBackData.length > 0 && callBackData[0] != null && callBackData[0] != Undefined.instance) {
            final Object error = callBackData[0];
            handleErrorResult(error);
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
     * @see #onErrorResult(Throwable)
     * @see #onDone
     */
    protected abstract void onSuccessResult(Object resultObj);

    /**
     * Must be called from the thread specified with {@link #getThreadToRun()}.
     */
    public void handleErrorResult(Object errorObj) {
        if (errorObj instanceof RhinoException) {
            final RhinoException rhinoException = (RhinoException) errorObj;
            onErrorResult(new RuntimeException(rhinoException.getMessage() + " : \n" + rhinoException.getScriptStackTrace() + '\n', rhinoException));
        } else if (errorObj instanceof Throwable) {
            onErrorResult((Throwable) errorObj);
        } else {
            onErrorResult(new RuntimeException("AppScripts function has returned an error: " + errorObj));
        }
    }

    /**
     * Called when "done()", that is called in JS script, contains errors. Normally JS should do nothing after done().
     *
     * @param error error passed to "done()" by JS script:
     *  - either RhinoException with JS stackTrace or
     *  - another exception itself
     *  - or wrapped into RuntimeException toString() representation of the thrown by JS object.
     */
    protected abstract void onErrorResult(Throwable error);

    /**
     * @return callers should call {@link #onDone(Object[]) method} on thread, returned by this method.
     */
    public ThreadToRun getThreadToRun() {
        return threadToRun;
    }

    /**
     * Thread to run correspondent {@link OnScriptExecutedCallBack} on.
     */
    static public enum ThreadToRun {
        DEFAULT, //thread, where JS called "done()" call-back. Either "event loop" or one of "AppScriptsWorkers" threads
        UI
    }
}
