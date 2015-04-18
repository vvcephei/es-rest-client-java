package com.bazaarvoice.elasticsearch.client.core.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.jackson.core.util.ByteArrayBuilder;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A few functions I needed similar to, but missing from,
 * {@link org.elasticsearch.common.xcontent.support.XContentMapValues}
 */
public class MapFunctions {
    public static Map<String, Object> toMapOrNull(final BytesReference bytes) {
        try {
            return SmileXContent.smileXContent.createParser(bytes).map();
        } catch (IOException e) {
            try {
                return JsonXContent.jsonXContent.createParser(bytes).map();
            } catch (IOException e1) {
                return null;
            }
        }
    }

    public static String nodeStringValue(@Nullable Object o) {
        Preconditions.checkNotNull(o);
        if (o instanceof String) {
            return (String) o;
        } else {
            return o.toString();
        }
    }

    public static <T> List<T> nodeListValue(@Nullable Object o, Class<T> contains) {
        Preconditions.checkNotNull(o);
        if (o instanceof List) {
            for (Object elem : (List) o) {
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
            throw new IllegalArgumentException(String.format("%s was expected to be a list but was %s", o, o.getClass()));
        }
    }

    public static BytesReference nodeBytesReferenceValue(final @Nullable Object o) {
        if (o == null) {
            return null;
        } else {
            return new BytesArray(readRaw(o));
        }
    }

    public static BytesRef nodeBytesRefValue(final @Nullable Object o) {
        if (o == null) {
            return null;
        } else {
            return new BytesRef(readRaw(o));
        }
    }

    //TODO test this. also is this really my best option?
    private static byte[] readRaw(final Object source) {
        ByteArrayBuilder byteArrayBuilder = new ByteArrayBuilder();
        try {
            XContentGenerator generator = JsonXContent.jsonXContent.createGenerator(byteArrayBuilder);
            if (source == null) {
                generator.writeNull();
            } else if (source instanceof Boolean) {
                generator.writeBoolean((Boolean) source);
            } else if (source instanceof Number) {
                if (source instanceof Double) {
                    generator.writeNumber((Double) source);
                } else if (source instanceof Float) {
                    generator.writeNumber((Float) source);
                } else if (source instanceof Integer) {
                    generator.writeNumber((Integer) source);
                } else if (source instanceof Long) { generator.writeNumber((Long) source); } else {
                    throw new IllegalStateException(String.format("unexpected numeric type %s of %s", source.getClass(), source));
                }
            } else if (source instanceof String) {
                generator.writeString((String) source);
            } else if (source instanceof List) {
                List<Object> objects = nodeListValue(source, Object.class);
                generator.writeStartArray();
                for (Object o : objects) {
                    generator.writeBinary(readRaw(o));
                }
                generator.writeEndArray();
            } else if (source instanceof Map) {
                Map<String, Object> map = nodeMapValue(source, String.class, Object.class);
                generator.writeStartObject();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    generator.writeRawField(entry.getKey(), readRaw(entry.getValue()), byteArrayBuilder);
                }
                generator.writeEndObject();
            }
            generator.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteArrayBuilder.toByteArray();
    }
}
