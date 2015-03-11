package com.bazaarvoice.elasticsearch.client.core.util;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.jackson.core.util.ByteArrayBuilder;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
        if (o instanceof Number){
            return ((Number)o).longValue();
        } else {
            throw new IllegalArgumentException(String.format("%s was expected to be a long but was %s", o, o.getClass()));
        }
    }

    public static boolean requireBoolean(@Nullable Object o) {
        Preconditions.checkNotNull(o);
        if (o instanceof Boolean) {
            return (Boolean) o;
        } else {
            throw new IllegalArgumentException(String.format("%s was expected to be a boolean but was %s", o, o.getClass()));
        }
    }

    public static <T> List<T> requireList(@Nullable Object o, Class<T> contains) {
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

    public static <K, V> Map<K, V> requireMap(final Object o, final Class<K> keyClass, final Class<V> valueClass) {
        Preconditions.checkNotNull(o);
        if (o instanceof Map) {
            for (Object entry : ((Map) o).entrySet()) {
                Map.Entry elem = (Map.Entry) entry;
                if (!keyClass.isAssignableFrom(elem.getKey().getClass())) {
                    throw new IllegalArgumentException(String.format("%s was expected to be a %s but was a %s", elem.getKey(), keyClass.getCanonicalName(), elem.getKey().getClass().getCanonicalName()));
                }
                if (!valueClass.isAssignableFrom(elem.getValue().getClass())) {
                    throw new IllegalArgumentException(String.format("%s was expected to be a %s but was a %s", elem.getValue(), valueClass.getCanonicalName(), elem.getValue().getClass().getCanonicalName()));
                }
            }
            //noinspection unchecked
            return (Map<K, V>) o;
        } else {
            throw new IllegalArgumentException(String.format("%s was expected to be a list but was %s", o, o.getClass()));
        }
    }

    public static BytesReference readBytesReference(final @Nullable Object o) {
        if (o == null) {
            return null;
        } else {
            return new BytesArray(readRaw(o));
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
                List<Object> objects = requireList(source, Object.class);
                generator.writeStartArray();
                for (Object o : objects) {
                    generator.writeBinary(readRaw(o));
                }
                generator.writeEndArray();
            } else if (source instanceof Map) {
                Map<String, Object> map = requireMap(source, String.class, Object.class);
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
