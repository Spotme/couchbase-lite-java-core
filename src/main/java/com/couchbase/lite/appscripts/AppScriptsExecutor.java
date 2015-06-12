package com.couchbase.lite.appscripts;

import java.util.Map;

public interface AppScriptsExecutor {

    AppScriptsExecutor newInstance();

    void runScript(String source, Map<String,Object> params, OnScriptExecutedCallBack callback);

    Map<String, Object> allAppScripts();

    String getActiveEvent();

}
