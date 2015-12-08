package com.couchbase.lite.appscripts;

import java.io.IOException;
import java.util.Map;

/**
 * todo: Instead of Exposing JS executor, we should better expose call-back,
 * which we can fully implement in Main SpotMe project.
 */
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

    String getJsSourceCode(String scriptPath) throws IOException;

    String getActiveEvent();

}