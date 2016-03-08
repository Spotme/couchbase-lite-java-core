package com.couchbase.lite.spotme;

import com.couchbase.lite.replicator.Replication;
import com.couchbase.lite.support.HttpClientFactory;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import cz.msebera.android.httpclient.client.CookieStore;
import cz.msebera.android.httpclient.client.HttpClient;
import cz.msebera.android.httpclient.conn.ClientConnectionManager;
import cz.msebera.android.httpclient.conn.params.ConnManagerParams;
import cz.msebera.android.httpclient.conn.params.ConnPerRoute;
import cz.msebera.android.httpclient.conn.routing.HttpRoute;
import cz.msebera.android.httpclient.conn.scheme.PlainSocketFactory;
import cz.msebera.android.httpclient.conn.scheme.Scheme;
import cz.msebera.android.httpclient.conn.scheme.SchemeRegistry;
import cz.msebera.android.httpclient.conn.ssl.SSLSocketFactory;
import cz.msebera.android.httpclient.conn.ssl.X509HostnameVerifier;
import cz.msebera.android.httpclient.cookie.Cookie;
import cz.msebera.android.httpclient.impl.client.DefaultHttpClient;
import cz.msebera.android.httpclient.impl.conn.tsccm.ThreadSafeClientConnManager;
import cz.msebera.android.httpclient.params.BasicHttpParams;
import cz.msebera.android.httpclient.params.HttpParams;

public class SpotMeHttpClientFactory implements HttpClientFactory {

    /**
     * Regular expression to check if the url is an ip address
     */
    final static String IS_URL_IP_ADDRESS = "^(http(s?):\\/\\/)?((.+?):(.+?)@)?(((([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])))+(\\/[^\\s]*)?";


    protected HttpClient httpClient;


    protected CookieStore mCookieStore;

    @Override
    public HttpClient getHttpClient() {
        if (httpClient == null) {
            httpClient = createHttpClient();
        }
        return httpClient;
    }

    public void addCookies(List<Cookie> cookies) {
        if (mCookieStore == null) {
            return;
        }
        synchronized (this) {
            for (Cookie cookie : cookies) {
                mCookieStore.addCookie(cookie);
            }
        }
    }

    public void deleteCookie(String name) {
        // since CookieStore does not have a way to delete an individual cookie, do workaround:
        // 1. get all cookies
        // 2. filter list to strip out the one we want to delete
        // 3. clear cookie store
        // 4. re-add all cookies except the one we want to delete
        if (mCookieStore == null) {
            return;
        }
        List<Cookie> cookies = mCookieStore.getCookies();
        List<Cookie> retainedCookies = new ArrayList<Cookie>();
        for (Cookie cookie : cookies) {
            if (!cookie.getName().equals(name)) {
                retainedCookies.add(cookie);
            }
        }
        mCookieStore.clear();
        for (Cookie retainedCookie : retainedCookies) {
            mCookieStore.addCookie(retainedCookie);
        }
    }

    public CookieStore getCookieStore() {
        return mCookieStore;
    }

    /**
     * Create a new HttpClient with or without ssl validation according to the url
     * @return
     */
    public static HttpClient createHttpClient() {
        final HostnameVerifier hostnameVerifier = new SpotMeHostnameVerifier();
        final SchemeRegistry registry = new SchemeRegistry();
        final SSLSocketFactory socketFactory = SSLSocketFactory.getSocketFactory();

        socketFactory.setHostnameVerifier((X509HostnameVerifier) hostnameVerifier);

        registry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
        registry.register(new Scheme("https", socketFactory, 443));

        final HttpParams params = new BasicHttpParams();

        ConnPerRoute connPerRoute = new ConnPerRoute() {

            //httpClient is used for DB only -> don't check host name
            @Override
            public int getMaxForRoute(HttpRoute httpRoute) {
                return Replication.EXECUTOR_THREAD_POOL_SIZE;
            }
        };

        ConnManagerParams.setMaxConnectionsPerRoute(params, connPerRoute);

        final ClientConnectionManager mgr = new ThreadSafeClientConnManager(params, registry);

        final DefaultHttpClient httpClient = new DefaultHttpClient(mgr, params);

        return httpClient;
    }

    /**
     * Verifies hostnames that are not IP addresses only (which are always allowed)
     */
    public static class SpotMeHostnameVerifier implements X509HostnameVerifier {
        private final X509HostnameVerifier defaultVerifier = SSLSocketFactory.STRICT_HOSTNAME_VERIFIER;

        @Override
        public boolean verify(String hostname, SSLSession session) {
            if (shouldDisableCertificateValidation(hostname)) return true;
            else return defaultVerifier.verify(hostname, session);
        }

        @Override
        public void verify(String hostname, SSLSocket sslSocket) throws IOException {
            if (shouldDisableCertificateValidation(hostname)) return;
            else defaultVerifier.verify(hostname, sslSocket);
        }

        @Override
        public void verify(String hostname, X509Certificate x509Certificate) throws SSLException {
            if (shouldDisableCertificateValidation(hostname)) return;
            else defaultVerifier.verify(hostname, x509Certificate);
        }

        @Override
        public void verify(String hostname, String[] cns, String[] subjectAlts) throws SSLException {
            if (shouldDisableCertificateValidation(hostname)) return;
            else defaultVerifier.verify(hostname, cns, subjectAlts);
        }
    }

    /**
     * Check if the certificate validation should be disabled
     * @param url
     * @return
     */
    public static boolean shouldDisableCertificateValidation(String url) {
        try {
            if (Pattern.matches(IS_URL_IP_ADDRESS, url)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}