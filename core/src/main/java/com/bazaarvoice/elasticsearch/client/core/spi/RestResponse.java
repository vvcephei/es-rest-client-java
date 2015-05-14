package com.bazaarvoice.elasticsearch.client.core.spi;

import java.io.InputStream;
import java.util.Set;


/**
 * An abstraction of web responses.
 * <p/>
 * Implementers can use any http client they like, configured however they want.
 */
public interface RestResponse {
    public boolean isSuccess();
    public int statusCode();
    public InputStream response();
    Set<String> contentTypeLowerCase();
}
