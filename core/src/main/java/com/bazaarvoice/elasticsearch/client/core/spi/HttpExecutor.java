package com.bazaarvoice.elasticsearch.client.core.spi;

import org.elasticsearch.common.util.concurrent.ListenableFuture;

import java.io.InputStream;
import java.net.URL;

/**
 * An abstraction of performing web requests.
 * <p/>
 * Implementers can use any http client they like, configured however they want.
 */
public interface HttpExecutor {
    public ListenableFuture<HttpResponse> get(URL url);
    public ListenableFuture<HttpResponse> delete(URL url);
    public ListenableFuture<HttpResponse> put(URL url, InputStream body);
    public ListenableFuture<HttpResponse> post(URL url, InputStream body);
}
