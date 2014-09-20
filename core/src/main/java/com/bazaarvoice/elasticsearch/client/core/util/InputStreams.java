package com.bazaarvoice.elasticsearch.client.core.util;

import org.elasticsearch.common.bytes.BytesReference;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class InputStreams {
    public static final InputStream of(BytesReference bytes) {
        return new ByteArrayInputStream(bytes.array());
    }
}
