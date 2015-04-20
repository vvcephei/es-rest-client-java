package org.elasticsearch.action.index;

import com.bazaarvoice.elasticsearch.client.core.spi.RestExecutor;
import com.bazaarvoice.elasticsearch.client.core.spi.RestResponse;
import com.bazaarvoice.elasticsearch.client.core.util.InputStreams;
import com.bazaarvoice.elasticsearch.client.core.util.UrlBuilder;
import org.elasticsearch.action.AbstractRestClientAction;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.util.concurrent.Futures;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.booleanToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.longToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.opTypeToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.replicationTypeToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.timeValueToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.versionTypeToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.writeConsistencyLevelToString;
import static com.bazaarvoice.elasticsearch.client.core.util.Validation.notNull;
import static org.elasticsearch.common.base.Optional.fromNullable;
import static org.elasticsearch.common.base.Optional.of;

/**
 * The inverse of {@link org.elasticsearch.rest.action.index.RestIndexAction}
 * @param <ResponseType>
 */
public class IndexRest<ResponseType> extends AbstractRestClientAction<IndexRequest, ResponseType> {
    public IndexRest(final String protocol, final String host, final int port, final RestExecutor executor, final Function<RestResponse, ResponseType> responseTransform) {
        super(protocol, host, port, executor, responseTransform);
    }

    @Override public ListenableFuture<ResponseType> act(IndexRequest request) {
        UrlBuilder url = UrlBuilder.create()
            .protocol(protocol).host(host).port(port)
            .path(notNull(request.index())).seg(notNull(request.type()))
            .paramIfPresent("routing", fromNullable(request.routing()))
            .paramIfPresent("parent", fromNullable(request.parent()))
            .paramIfPresent("timestamp", fromNullable(request.timestamp()))
            .paramIfPresent("ttl", (request.ttl() == -1) ? Optional.<String>absent() : of(Long.toString(request.ttl())))
            .paramIfPresent("timeout", fromNullable(request.timeout()).transform(timeValueToString))
            .paramIfPresent("refresh", fromNullable(request.refresh()).transform(booleanToString))
            .paramIfPresent("version", fromNullable(request.version()).transform(longToString))
            .paramIfPresent("version_type", fromNullable(request.versionType()).transform(versionTypeToString))
            .paramIfPresent("op_type", fromNullable(request.opType()).transform(opTypeToString))
            .paramIfPresent("replication", fromNullable(request.replicationType()).transform(replicationTypeToString))
            .paramIfPresent("consistency", fromNullable(request.consistencyLevel()).transform(writeConsistencyLevelToString)
            );

        // source:
        if (request.id() == null) {
            // auto id creation
            return Futures.transform(executor.post(url.url(), InputStreams.of(request.safeSource())), responseTransform);
        } else {
            return Futures.transform(executor.put(url.seg(request.id()).url(), InputStreams.of(request.safeSource())), responseTransform);
        }
    }
}
