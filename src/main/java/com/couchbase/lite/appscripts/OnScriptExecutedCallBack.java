package com.couchbase.lite.appscripts;

import com.getsentry.raven.Raven;

import org.mozilla.javascript.RhinoException;
import org.mozilla.javascript.Undefined;

import ro.isdc.wro.extensions.script.RhinoUtils;

/**
 * Call-back, that is called when corespondent call-back function is called in JS script.
 * Usually corespondent call-back function is named done() and is last param in JS script function.
 */
public abstract class OnScriptExecutedCallBack {
    /**
     * Runs call-back {@link #onSuccessResult(Object)} or {@link #onErrorResult(Throwable)} using desired thread.
     */
    final Threader threader;

    /**
     * Raven to report appscirpts errors to Sentry (if set).
     */
    Raven raven;

    public OnScriptExecutedCallBack(Threader threader) {
        this.threader = threader;
    }

    public OnScriptExecutedCallBack() {
        this(DefaultThreader.getInstance());
    }


    public void setRaven(Raven raven) {
        this.raven = raven;
    }

    /**
     * Called when "done()" is called in JS script. Normally JS should do nothing after it.
     *
     * Implementers should consider using {@link #onErrorResult(Throwable)} and {@link #onSuccessResult(Object)} instead.
     * Default implementation dispatches incoming data to these methods and run them on desired thread using {@link #threader}.
     *
     * This call-back should be @Overridden only when Implementer do not care about result of "done()",
     * and just need to know, that execution of "App Script" is finished.
     *
     * @param callBackData data passed to "done()" by JS script.
     */
    public final void onDone(Object[] callBackData) {
        if (callBackData.length > 0 && callBackData[0] != null && callBackData[0] != Undefined.instance) {
            final Object jsErrorObj = callBackData[0];
            handleErrorResult(jsErrorObj);
        } else {
            final Object respObj;
            if (callBackData.length > 1 && callBackData[1] != Undefined.instance) {
                respObj = callBackData[1];
            } else {
                respObj = null;
            }
            handleSuccessResult(respObj);
        }
    }

    /**
     * Called when "done()" is called in JS script without error data. Normally JS should do nothing after it.
     *
     * Implementers should use {@link #onSuccessResult(Object)} instead.
     * Default implementation dispatches incoming data to this method and run it on proper thread using {@link #threader}.
     *
     *
     * @param resultObj result, passed to "done()" by JS script.
     */
    protected void handleSuccessResult(final Object resultObj) {
        final Runnable onSuccessRunnable = new Runnable() {
            @Override
            public void run() {
                onSuccessResult(resultObj);
            }
        };

        threader.execute(onSuccessRunnable);
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
     * Called when "done()" is called in JS script. Normally JS should do nothing after it.
     *
     * Implementers should use {@link #onErrorResult(Throwable)}.
     * Default implementation dispatches incoming data to this method and run it on proper thread using {@link #threader}.
     *
     * @param jsErrorObj error object, passed to "done()" bu JS script.
     */
    public void handleErrorResult(final Object jsErrorObj) {
        final Runnable onErrorResult = new Runnable() {
            @Override
            public void run() {
                final Throwable jsError;
                if (jsErrorObj instanceof RhinoException) {
                    final RhinoException rhinoException = (RhinoException) jsErrorObj;
                    jsError = new RuntimeException(rhinoException.getMessage() + " : \n" + rhinoException.getScriptStackTrace() + '\n', rhinoException);
                } else if (jsErrorObj instanceof Throwable) {
                    jsError = (Throwable) jsErrorObj;
                } else {
                    jsError = new RuntimeException("AppScripts function has returned an error: " + RhinoUtils.toJson(jsErrorObj, true));
                }

                if (raven != null) raven.sendException(jsError);

                onErrorResult(jsError);
            }
        };

        threader.execute(onErrorResult);
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
     * Runs call-back {@link #onSuccessResult(Object)} or {@link #onErrorResult(Throwable)} using specific thread.
     */
    public static interface Threader {
        public void execute(Runnable callBack);
    }

    /**
     * Executes call-back Runnable on the same thread, where {@link #execute(Runnable)} is called.
     */
    public static class DefaultThreader implements Threader {
        private static final DefaultThreader instance = new DefaultThreader();

        private DefaultThreader() {
        }

        public static DefaultThreader getInstance() {
            return instance;
        }

        @Override
        public void execute(Runnable callBack) {
            callBack.run();
        }
    }
}
