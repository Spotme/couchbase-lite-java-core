package com.couchbase.lite;

import java.util.Map;

public interface AppScriptsCompiler {

    public AppScriptsCompiler newInstance();

    public void runScript(String source, Map<String,Object> params, AppScriptsRunnable callback);

}
