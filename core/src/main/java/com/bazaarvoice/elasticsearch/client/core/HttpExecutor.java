package com.bazaarvoice.elasticsearch.client.core;

import org.elasticsearch.common.util.concurrent.ListenableFuture;

import java.io.InputStream;
import java.net.URL;

public interface HttpExecutor {
    public ListenableFuture<HttpResponse> get(URL url);
    public ListenableFuture<HttpResponse> put(URL url, InputStream body);
    public ListenableFuture<HttpResponse> post(URL url, InputStream body);
}
