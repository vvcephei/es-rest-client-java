package org.elasticsearch.action;

import com.bazaarvoice.elasticsearch.client.core.spi.HttpExecutor;
import com.bazaarvoice.elasticsearch.client.core.spi.HttpResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.util.concurrent.FutureCallback;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

public abstract class AbstractRestClientAction<Request, Response> {
    protected final String protocol;
    protected final String host;
    protected final int port;
    protected final HttpExecutor executor;
    protected final Function<HttpResponse, Response> responseTransform;

    public AbstractRestClientAction(final String protocol, final String host, final int port, final HttpExecutor executor, final Function<HttpResponse, Response> responseTransform) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;

        this.executor = executor;
        this.responseTransform = responseTransform;
    }

    public abstract ListenableFuture<Response> act(Request request);

    public FutureCallback<Response> callback(ActionListener<Response> listener) {
        return new NotifyingCallback<Response>(listener);
    }
}
