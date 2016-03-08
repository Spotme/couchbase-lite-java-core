package com.couchbase.lite.support;

import com.couchbase.lite.Database;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import cz.msebera.android.httpclient.client.HttpClient;
import cz.msebera.android.httpclient.client.methods.HttpPost;
import cz.msebera.android.httpclient.client.methods.HttpPut;
import cz.msebera.android.httpclient.client.methods.HttpUriRequest;
import org.apache.http.entity.mime.MultipartEntity;

public class RemoteMultipartRequest extends RemoteRequest {

    private MultipartEntity multiPart;

    public RemoteMultipartRequest(ScheduledExecutorService workExecutor,
                                  HttpClientFactory clientFactory, String method, URL url,
                                  MultipartEntity multiPart, Database db, Map<String, Object> requestHeaders, RemoteRequestCompletionBlock onCompletion) {
        super(workExecutor, clientFactory, method, url, null, db, requestHeaders, onCompletion);
        this.multiPart = multiPart;
    }

    @Override
    public void run() {

        HttpClient httpClient = clientFactory.getHttpClient();

        preemptivelySetAuthCredentials(httpClient);

        HttpUriRequest request = null;
        if (method.equalsIgnoreCase("PUT")) {
            HttpPut putRequest = new HttpPut(url.toExternalForm());
            putRequest.setEntity(multiPart);
            request = putRequest;

        } else if (method.equalsIgnoreCase("POST")) {
            HttpPost postRequest = new HttpPost(url.toExternalForm());
            postRequest.setEntity(multiPart);
            request = postRequest;
        } else {
            throw new IllegalArgumentException("Invalid request method: " + method);
        }

        request.addHeader("Accept", "*/*");
	    addRequestHeaders(request);

        executeRequest(httpClient, request);

    }


}
