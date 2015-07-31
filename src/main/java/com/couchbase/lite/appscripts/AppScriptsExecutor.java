package com.couchbase.lite.appscripts;

import java.util.Map;

public interface AppScriptsExecutor {

    AppScriptsExecutor newInstance();

    /**
     * Execute appscripts source code
     *
     * @param scriptSource
     * @param params
     * @param sourceName a string describing the source, such as a filename
     * @param callbackFunction should be called in JS when execution is done.
     */
    public void runScript(final String scriptSource, final Map<String, Object> params, final String sourceName, final OnScriptExecutedCallBack callbackFunction);

    Map<String, Object> allAppScripts();

    String getActiveEvent();

}
