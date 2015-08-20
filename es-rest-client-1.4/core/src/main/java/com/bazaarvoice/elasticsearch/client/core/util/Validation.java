package com.bazaarvoice.elasticsearch.client.core.util;

import static org.elasticsearch.common.Preconditions.checkNotNull;

public class Validation {
    public static <T> T notNull(T t) {
        checkNotNull(t);
        return t;
    }
}
