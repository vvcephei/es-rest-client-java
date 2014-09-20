package com.bazaarvoice.elasticsearch.client.core;

import java.io.InputStream;

public interface ResponseReader<T> {
    public T parse(InputStream response);
}
