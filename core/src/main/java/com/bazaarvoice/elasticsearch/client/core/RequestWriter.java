package com.bazaarvoice.elasticsearch.client.core;

import java.io.OutputStream;

public interface RequestWriter<T> {
    public OutputStream write(T t);
}
