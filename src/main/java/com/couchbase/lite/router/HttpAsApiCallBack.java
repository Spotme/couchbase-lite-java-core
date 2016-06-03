package com.couchbase.lite.router;

import com.couchbase.lite.Manager;
import com.couchbase.lite.Status;
import com.couchbase.lite.appscripts.AppScriptsExecutor;
import com.couchbase.lite.appscripts.OnScriptExecutedCallBack;
import com.couchbase.lite.router.Router.HttpJsApiCallBack;
import com.couchbase.lite.util.Log;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * App-script based implementation of Http JS api
 */
public class HttpAsApiCallBack implements HttpJsApiCallBack {

    /**
     *  call-back for _api/ thru http.
     *
     *  Performs business logic in AppScript and return raw result from JS.
     *
     * @throws ApiCallException in case of wrong incoming data or at execution of _api/ call.
     */
    @Override
    public Object performApiCall(final String url, boolean isGetRequest, URLConnection connection) throws ApiCallException {
        AppScriptsExecutor appScriptsExecutor = Router.appScriptsExecutor;

        if (url.split("_api/").length < 2) {
            throw new ApiCallException("Unable to find api version", Status.UNKNOWN);
        }

        final String[] elements = url.split("_api/")[1].split("/");
        final String apiVersion = elements[0];

        if (!"v1".equals(apiVersion)) {
            throw new ApiCallException("Unable to find api version", Status.UNKNOWN);
        }

        if (elements.length < 3) {
            throw new ApiCallException("Event id has to be provided", Status.UNKNOWN);
        }

        final String eid = elements[2];
        if (!appScriptsExecutor.getActiveEvent().equals(eid)) {
            throw new ApiCallException("Event not activated", Status.UNKNOWN);
        }

        if (elements.length < 4) {
            throw new ApiCallException("action has to be provided (appscripts?)", Status.UNKNOWN);
        }

        String action = elements[3];
        if (action.contains("?")) action = action.split("\\?")[0];

        if (!"appscripts".equals(action)) {
            throw new ApiCallException("Unknown action", Status.UNKNOWN);
        }


        final AppScriptsExecutor compiler = appScriptsExecutor.newInstance();
        String JsFunctionSource = "";
        Map<String, Object> params = new HashMap<>();

        if (elements.length == 4) {
            final String paramsString = connection.getURL().getQuery();
            JsFunctionSource = slurp(connection.getRequestInputStream());
            try {
                if (paramsString != null) params = splitQuery(paramsString);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        } else {
            //take the function from the input stream (debug mode)
            final String scriptPath = url.split("appscripts/")[1].split("\\?")[0];
            try {
                JsFunctionSource = compiler.getJsSourceCode(scriptPath);
            } catch (IOException e) {
                throw new ApiCallException("Unable to perform _api/ call: " + e.getMessage(), Status.UNKNOWN);
            }

            if (!isGetRequest) {
                try {
                    params = Manager.getObjectMapper().readValue(connection.getRequestInputStream(), Map.class);
                    params = (Map<String, Object>) params.get("params");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                final String paramsString = connection.getURL().getQuery();
                try {
                    if (paramsString != null) params = splitQuery(paramsString);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            final CountDownLatch jsFinishedLatch = new CountDownLatch(1);
            final AtomicReference<Object> resultReference = new AtomicReference<>();
            final AtomicReference<Throwable> errorReference = new AtomicReference<>();

            compiler.runScript(JsFunctionSource, params, url, new OnScriptExecutedCallBack() {

                @Override
                protected void onSuccessResult(Object resultObj) {
                    resultReference.set(resultObj);
                    jsFinishedLatch.countDown();
                }

                @Override
                protected void onErrorResult(Throwable error) {
                    errorReference.set(error);
                    jsFinishedLatch.countDown();
                }

            });

            final int maxTimeToWaitJsExecution = 30; //max wait time is randomly picked for now
            jsFinishedLatch.await(maxTimeToWaitJsExecution, TimeUnit.SECONDS);

            final Throwable errorFromJs = errorReference.get();
            if (errorFromJs == null) {
                return resultReference.get();
            } else {
                final String errorMsg = "Request to " + url + " returned with error: ";
                Log.w(Log.TAG_ROUTER, errorMsg, errorFromJs);
                throw new ApiCallException(errorMsg + errorFromJs.getMessage(), Status.INTERNAL_SERVER_ERROR);
            }
        } catch (Exception e) {
            throw new ApiCallException("Unable to execute AppScript: " + e.getMessage(), Status.DB_ERROR);
        }
    }


    /**
     * Converts inputstream to string
     * @param is
     * @return
     */
    private static String slurp(final InputStream is) {
        byte[] bytes = new byte[1000];

        StringBuilder out = new StringBuilder();

        int numRead = 0;
        try {
            while ((numRead = is.read(bytes)) >= 0) {
                out.append(new String(bytes, 0, numRead));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return out.toString();
    }

    /**
     * Converts query params to map
     * @param url
     * @return
     * @throws UnsupportedEncodingException
     */
    public static Map<String, Object> splitQuery(String url) throws UnsupportedEncodingException {
        final Map<String, Object> queryPairs = new LinkedHashMap<>();
        final String[] pairs = url.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            queryPairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
        }
        return queryPairs;
    }
}
