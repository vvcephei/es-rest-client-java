package com.bazaarvoice.elasticsearch.client.core.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.base.Throwables;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.jackson.core.util.ByteArrayBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A few functions I needed similar to, but missing from,
 * {@link org.elasticsearch.common.xcontent.support.XContentMapValues}
 */
public class MapFunctions {
    public static Map<String, Object> toMap(final BytesReference bytes) {
        try {
            return SmileXContent.smileXContent.createParser(bytes).map();
        } catch (IOException e) {
            try {
                return JsonXContent.jsonXContent.createParser(bytes).map();
            } catch (IOException e1) {
                throw new RuntimeException("Couldn't parse bytes as either SMILE or JSON", e);
            }
        }
    }

    public static String nodeStringValue(@Nullable Object o) {
        Preconditions.checkNotNull(o);
        if (o instanceof String) {
            return (String) o;
        } else {
            return Objects.toString(o);
        }
    }

    public static <T> List<T> nodeListValue(@Nullable Object o, Class<T> contains) {
        Preconditions.checkNotNull(o);
        if (o instanceof List) {
            for (Object elem : (List) o) {
                if (elem != null && !contains.isAssignableFrom(elem.getClass())) {
                    throw new IllegalArgumentException(String.format("%s was expected to be a %s but was a %s", elem, contains.getCanonicalName(), elem.getClass().getCanonicalName()));
                }
            }
            //noinspection unchecked
            return (List<T>) o;
        } else {
            throw new IllegalArgumentException(String.format("%s was expected to be a list but was %s", o, o.getClass()));
        }
    }

    public static <K, V> Map<K, V> nodeMapValue(final Object o, final Class<K> keyClass, final Class<V> valueClass) {
        Preconditions.checkNotNull(o);
        if (o instanceof Map) {
            for (Object entry : ((Map) o).entrySet()) {
                Map.Entry elem = (Map.Entry) entry;
                if (!keyClass.isAssignableFrom(elem.getKey().getClass())) {
                    throw new IllegalArgumentException(String.format("%s was expected to be a %s but was a %s", elem.getKey(), keyClass.getCanonicalName(), elem.getKey().getClass().getCanonicalName()));
                }
                final Object value = elem.getValue();
                // null is assignable to every type
                if (value != null && !valueClass.isAssignableFrom(value.getClass())) {
                    throw new IllegalArgumentException(String.format("%s was expected to be a %s but was a %s", value, valueClass.getCanonicalName(), value.getClass().getCanonicalName()));
                }
            }
            //noinspection unchecked
            return (Map<K, V>) o;
        } else {
            throw new IllegalArgumentException(String.format("%s was expected to be a map but was %s", o, o.getClass()));
        }
    }

    // assumption: _source is always a map
    public static BytesReference nodeBytesReferenceForMapValue(final @Nullable Map<String, Object> o) {
        if (o == null) {
            return null;
        } else {
            try {
                return XContentFactory.jsonBuilder().map(o).bytes();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
