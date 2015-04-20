package com.bazaarvoice.elasticsearch.client.core.spi;

import org.elasticsearch.common.util.concurrent.ListenableFuture;

import java.io.InputStream;
import java.net.URL;

/**
 * An abstraction of performing web requests.
 * <p/>
 * Implementers can use any http client they like, configured however they want.
 */
public interface RestExecutor {
    public ListenableFuture<RestResponse> get(URL url);
    public ListenableFuture<RestResponse> delete(URL url);
    public ListenableFuture<RestResponse> put(URL url, InputStream body);
    public ListenableFuture<RestResponse> post(URL url, InputStream body);
}
