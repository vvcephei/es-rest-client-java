package com.bazaarvoice.elasticsearch.client.core.util;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestRequest;

import java.util.Map;

public class HttpRestRequest extends RestRequest {
    @Override public Method method() {
        return null;
    }

    @Override public String uri() {
        return null;
    }

    @Override public String rawPath() {
        return null;
    }

    @Override public boolean hasContent() {
        return false;
    }

    @Override public boolean contentUnsafe() {
        return false;
    }

    @Override public BytesReference content() {
        return null;
    }

    @Override public String header(final String name) {
        return null;
    }

    @Override public Iterable<Map.Entry<String, String>> headers() { return null; }

    @Override public boolean hasParam(final String key) {
        return false;
    }

    @Override public String param(final String key) {
        return null;
    }

    @Override public String param(final String key, final String defaultValue) {
        return null;
    }

    @Override public String[] paramAsStringArray(final String key, final String[] defaultValue) {
        return new String[0];
    }

    @Override public float paramAsFloat(final String key, final float defaultValue) {
        return 0;
    }

    @Override public int paramAsInt(final String key, final int defaultValue) {
        return 0;
    }

    @Override public long paramAsLong(final String key, final long defaultValue) {
        return 0;
    }

    @Override public boolean paramAsBoolean(final String key, final boolean defaultValue) {
        return false;
    }

    @Override public Boolean paramAsBooleanOptional(final String key, final Boolean defaultValue) {
        return null;
    }

    @Override public TimeValue paramAsTime(final String key, final TimeValue defaultValue) {
        return null;
    }

    @Override public ByteSizeValue paramAsSize(final String key, final ByteSizeValue defaultValue) {
        return null;
    }

    @Override public Map<String, String> params() {
        return null;
    }
}
