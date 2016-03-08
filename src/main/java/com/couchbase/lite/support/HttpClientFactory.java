package com.couchbase.lite.support;


import java.util.List;

import cz.msebera.android.httpclient.client.CookieStore;
import cz.msebera.android.httpclient.client.HttpClient;
import cz.msebera.android.httpclient.cookie.Cookie;

public interface HttpClientFactory {
	HttpClient getHttpClient();
    public void addCookies(List<Cookie> cookies);
    public void deleteCookie(String name);
    public CookieStore getCookieStore();
}
