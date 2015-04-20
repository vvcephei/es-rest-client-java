package org.elasticsearch.action.delete;

import com.bazaarvoice.elasticsearch.client.core.spi.RestExecutor;
import com.bazaarvoice.elasticsearch.client.core.spi.RestResponse;
import com.bazaarvoice.elasticsearch.client.core.util.UrlBuilder;
import org.elasticsearch.action.AbstractRestClientAction;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.util.concurrent.Futures;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.booleanToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.longToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.replicationTypeToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.timeValueToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.versionTypeToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.writeConsistencyLevelToString;
import static com.bazaarvoice.elasticsearch.client.core.util.Validation.notNull;
import static org.elasticsearch.common.base.Optional.fromNullable;

/**
 * The inverse of {@link org.elasticsearch.rest.action.delete.RestDeleteAction}
 *
 * @param <ResponseType>
 */
public class DeleteRest<ResponseType> extends AbstractRestClientAction<DeleteRequest, ResponseType> {

    public DeleteRest(final String protocol, final String host, final int port, final RestExecutor executor, final Function<RestResponse, ResponseType> responseTransform) {
        super(protocol, host, port, executor, responseTransform);
    }

    @Override public ListenableFuture<ResponseType> act(final DeleteRequest request) {
        UrlBuilder url = UrlBuilder.create()
            .protocol(protocol).host(host).port(port)
            .path(notNull(request.index())).seg(notNull(request.type())).seg(notNull(request.id()))
            .paramIfPresent("routing", fromNullable(request.routing()))
                // note parent(string) seems just to set the routing, so we don't need to provide it here
            .paramIfPresent("timeout", fromNullable(request.timeout()).transform(timeValueToString))
            .paramIfPresent("refresh", fromNullable(request.refresh()).transform(booleanToString))
            .paramIfPresent("version", fromNullable(request.version()).transform(longToString))
            .paramIfPresent("version_type", fromNullable(request.versionType()).transform(versionTypeToString))
            .paramIfPresent("replication", fromNullable(request.replicationType()).transform(replicationTypeToString))
            .paramIfPresent("consistency", fromNullable(request.consistencyLevel()).transform(writeConsistencyLevelToString));
        return Futures.transform(executor.delete(url.url()), responseTransform);
    }

}
