package com.couchbase.lite;

import java.util.Map;

public interface AppScriptsExecutor {

    AppScriptsExecutor newInstance();

    void runScript(String source, Map<String,Object> params, AppScriptsRunnable callback);

    Map<String, Object> allScripts();

    String getActiveEvent();

}
