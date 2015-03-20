package com.bazaarvoice.elasticsearch.client;

import com.bazaarvoice.elasticsearch.client.core.HttpClient;
import com.sun.jersey.api.client.Client;
import org.elasticsearch.common.util.concurrent.MoreExecutors;

import java.util.concurrent.ExecutorService;

/**
 * Factory to provide {@link com.bazaarvoice.elasticsearch.client.core.HttpClient}s
 * which use Jersey as the transport.
 * <p/>
 * Note that you give us the client, so you control the configuration.
 * <p/>
 * TODO Once {@link com.bazaarvoice.elasticsearch.client.core.HttpClient} changes
 * to be a traditional ES "thick" client, we should let our callers decide if they want
 * one client per endpoint or one for all. Perhaps add another method that takes a
 * {@code Provider<Client>} of some kind.
 */
public class JerseyHttpClientFactory {
    public static HttpClient client(String protocol, String host, int port, Client jerseyClient, ExecutorService executor) {
        return HttpClient.withExecutor(protocol, host, port, new JerseyHttpExecutor(jerseyClient, MoreExecutors.listeningDecorator(executor)));
    }
}
