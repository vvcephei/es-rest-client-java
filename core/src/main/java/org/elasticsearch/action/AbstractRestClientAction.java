package org.elasticsearch.action;

import com.bazaarvoice.elasticsearch.client.core.spi.HttpExecutor;
import org.elasticsearch.common.util.concurrent.FutureCallback;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

public abstract class AbstractRestClientAction<Request, Response> {
    protected final String protocol;
    protected final String host;
    protected final int port;
    protected final HttpExecutor executor;

    public AbstractRestClientAction(final String protocol, final String host, final int port, final HttpExecutor executor) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;

        this.executor = executor;
    }

    public abstract ListenableFuture<Response> act(Request request);

    public abstract FutureCallback<Response> callback(ActionListener<Response> listener);
}
