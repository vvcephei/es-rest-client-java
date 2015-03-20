package com.bazaarvoice.elasticsearch.client;

import com.bazaarvoice.elasticsearch.client.core.spi.HttpExecutor;
import com.bazaarvoice.elasticsearch.client.core.spi.HttpResponse;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.base.Throwables;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ListeningExecutorService;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.Callable;

/**
 * An executor which delegates to Jersey.
 * <p/>
 * TODO It seems like there should be a way to use Jersey's {@link com.sun.jersey.api.client.AsyncWebResource} and not need an executor of our own,
 * TODO but I couldn't figure out how to make it happen.
 */
class JerseyHttpExecutor implements HttpExecutor {
    private final Client client;
    private final ListeningExecutorService executorService;

    private static final Function<ClientResponse, HttpResponse> toHttpResponse = new Function<ClientResponse, HttpResponse>() {
        @Override public HttpResponse apply(final ClientResponse clientResponse) { return new JerseyResponse(clientResponse); }
    };

    JerseyHttpExecutor(final Client client, final ListeningExecutorService executorService) {
        this.client = client;
        this.executorService = executorService;
    }

    @Override public ListenableFuture<HttpResponse> get(final URL url) {
        // I feel like there's got to be a way to wrap the future with a listenable future, rather than submitting
        // to an executor. Just being expedient here...
        return executorService.submit(new Callable<HttpResponse>() {
            @Override public HttpResponse call() throws Exception {
                return toHttpResponse.apply(toWebResource(url).get(ClientResponse.class));
            }
        });
    }

    @Override public ListenableFuture<HttpResponse> delete(final URL url) {
        return executorService.submit(new Callable<HttpResponse>() {
            @Override public HttpResponse call() throws Exception {
                return toHttpResponse.apply(toWebResource(url).delete(ClientResponse.class));
            }
        });
    }


    @Override public ListenableFuture<HttpResponse> put(final URL url, final InputStream body) {
        return executorService.submit(new Callable<HttpResponse>() {
            @Override public HttpResponse call() throws Exception {
                return toHttpResponse.apply(toWebResource(url).put(ClientResponse.class, body));
            }
        });
    }

    @Override public ListenableFuture<HttpResponse> post(final URL url, final InputStream body) {
        return executorService.submit(new Callable<HttpResponse>() {
            @Override public HttpResponse call() throws Exception {
                return toHttpResponse.apply(toWebResource(url).post(ClientResponse.class, body));
            }
        });
    }

    private WebResource toWebResource(final URL url) {
        try {
            return client.resource(url.toURI());
        } catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }
    }
}
