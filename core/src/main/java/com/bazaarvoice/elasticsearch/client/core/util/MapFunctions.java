package com.bazaarvoice.elasticsearch.client.core.util;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;

import java.util.List;

public class MapFunctions {
    public static String requireString(@Nullable Object o) {
        Preconditions.checkNotNull(o);
        if (o instanceof String) {
            return (String) o;
        } else {
            throw new IllegalArgumentException(String.format("%s was expected to be a string but was %s", o, o.getClass()));
        }
    }

    public static long requireLong(@Nullable Object o) {
        Preconditions.checkNotNull(o);
        if (o instanceof Long) {
            return (Long) o;
        } else {
            throw new IllegalArgumentException(String.format("%s was expected to be a long but was %s", o, o.getClass()));
        }
    }

    public static <T> List<T> requireList(@Nullable Object o, Class<T> contains) {
        Preconditions.checkNotNull(o);
        if (o instanceof List) {
            for (Object elem : (List) o){
                if (!contains.isAssignableFrom(elem.getClass())) {
                    throw new IllegalArgumentException(String.format("%s was expected to be a %s but was a %s", elem, contains.getCanonicalName(), elem.getClass().getCanonicalName()));
                }
            }
            //noinspection unchecked
            return (List<T>) o;
        } else {
            throw new IllegalArgumentException(String.format("%s was expected to be a list but was %s", o, o.getClass()));
        }
    }
}
