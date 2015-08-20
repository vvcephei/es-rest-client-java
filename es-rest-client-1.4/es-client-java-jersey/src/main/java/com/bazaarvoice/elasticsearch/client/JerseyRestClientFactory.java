package com.bazaarvoice.elasticsearch.client;

import com.bazaarvoice.elasticsearch.client.core.RestClient;
import com.sun.jersey.api.client.Client;
import org.elasticsearch.common.util.concurrent.MoreExecutors;

import java.util.concurrent.ExecutorService;

/**
 * Factory to provide {@link RestClient}s
 * which use Jersey as the transport.
 * <p/>
 * Note that you give us the client, so you control the configuration.
 * <p/>
 * TODO Once {@link RestClient} changes
 * to be a traditional ES "thick" client, we should let our callers decide if they want
 * one client per endpoint or one for all. Perhaps add another method that takes a
 * {@code Provider<Client>} of some kind.
 */
public class JerseyRestClientFactory {
    public static RestClient client(String protocol, String host, int port, Client jerseyClient, ExecutorService executor) {
        return RestClient.withExecutor(protocol, host, port, new JerseyRestExecutor(jerseyClient, MoreExecutors.listeningDecorator(executor)));
    }
}
