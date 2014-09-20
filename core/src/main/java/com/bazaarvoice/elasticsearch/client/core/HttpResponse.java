package com.bazaarvoice.elasticsearch.client.core;

import java.io.InputStream;

public interface HttpResponse {
    public boolean isSuccess();
    public int statusCode();
    public InputStream response();
}
