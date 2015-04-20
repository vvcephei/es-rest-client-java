package com.bazaarvoice.elasticsearch.client;

import com.bazaarvoice.elasticsearch.client.core.spi.RestResponse;
import com.sun.jersey.api.client.ClientResponse;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.collect.ImmutableSet;

import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.Set;

import static org.elasticsearch.common.collect.Iterables.transform;

/**
 * Adapts a Jersey {@link com.sun.jersey.api.client.ClientResponse}
 * to {@link RestResponse}.
 */
class JerseyResponse implements RestResponse {
    private final ClientResponse delegate;
    private static final Function<String, String> toLowerCaseFn = new Function<String, String>() {
        @Override public String apply(final String s) {
            return s.toLowerCase();
        }
    };

    JerseyResponse(final ClientResponse delegate) {this.delegate = delegate;}

    @Override public boolean isSuccess() {
        return Response.Status.Family.SUCCESSFUL.equals(delegate.getClientResponseStatus().getFamily());
    }

    @Override public int statusCode() {
        return delegate.getStatus();
    }

    @Override public InputStream response() {
        return delegate.getEntityInputStream();
    }

    @Override public Set<String> contentTypeLowerCase() {
        return ImmutableSet.copyOf(transform(delegate.getHeaders().get("Content-Type"), toLowerCaseFn));
    }

}