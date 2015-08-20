package com.bazaarvoice.elasticsearch.client.core.util;

import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.base.Splitter;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Lists;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;

import static org.elasticsearch.common.Preconditions.checkNotNull;

/**
 * A convenience class. Allows you to build urls using method chaining and without
 * external dependencies (except a few guice utilities via ES).
 *
 * Not guaranteed to be hardened. I threw it together to make implementing the
 * REST calls easier. I'm planning to pull this into a separate project
 * and harden it later.
 */
public class UrlBuilder {
    private final String protocol;
    private final String host;
    private final Integer port;
    private final String path;
    private final String query;

    public static UrlBuilder create() {
        return new UrlBuilder(null, null, null, null, null);
    }

    private UrlBuilder(String protocol, String host, Integer port, String path, String query) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;
        this.path = path;
        this.query = query;
    }

    public UrlBuilder protocol(final String protocol) {
        return new UrlBuilder(protocol, host, port, path, query);
    }

    public UrlBuilder host(final String host) {
        return new UrlBuilder(protocol, host, port, path, query);
    }

    public UrlBuilder port(final int port) {
        return new UrlBuilder(protocol, host, port, path, query);
    }

    public UrlBuilder path(final String... path) {
        List<String> segments = Lists.newArrayList();
        for (String pathSegment : path) {
            List<String> split = ImmutableList.copyOf(Splitter.on('/').omitEmptyStrings().trimResults().split(pathSegment));
            segments.addAll(split);
        }
        String finalPath = Joiner.on('/').join(segments);
        return new UrlBuilder(protocol, host, port, finalPath, query);
    }

    public UrlBuilder seg(final String... segments) {
        String partialPath = Joiner.on('/').skipNulls().join(segments);
        List<String> split = ImmutableList.copyOf(Splitter.on('/').omitEmptyStrings().trimResults().split(partialPath));
        String finalPartialPath = Joiner.on('/').join(split);

        String originalPath = path == null ? "" : path;
        return new UrlBuilder(protocol, host, port, originalPath + "/" + finalPartialPath, query);
    }

    public UrlBuilder paramIfPresent(String key, Optional<String> value) {
        if (value.isPresent()) {
            checkNotNull(key);
            checkNotNull(value.get());
            String newParam = key + "=" + value.get();
            String newQuery = query == null ? newParam : query + "&" + newParam;
            return new UrlBuilder(protocol, host, port, path, newQuery);
        } else {
            return this;
        }
    }

    public static String urlEncode(final String key) {
        try {
            return URLEncoder.encode(key, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String[] urlEncodeAll(final String[] keys) {
        final String[] result = new String[keys.length];
        for (int i = 0; i < keys.length; i++) {
            result[i] = urlEncode(keys[i]);
        }
        return result;
    }

    public URL url() {
        checkNotNull(protocol);
        checkNotNull(host);
        checkNotNull(port);
        checkNotNull(path);
        String thePath = query == null ? path : path + "?" + query;
        try {
            return new URL(protocol, host, port, "/" + thePath);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
