package org.elasticsearch.action.index;

import com.bazaarvoice.elasticsearch.client.core.HttpExecutor;
import com.bazaarvoice.elasticsearch.client.core.HttpResponse;
import com.bazaarvoice.elasticsearch.client.core.util.InputStreams;
import com.bazaarvoice.elasticsearch.client.core.util.UrlBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.util.concurrent.FutureCallback;
import org.elasticsearch.common.util.concurrent.Futures;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireBoolean;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireLong;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireString;
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

public class IndexRest {
    private final String protocol;
    private final String host;
    private final int port;
    private final HttpExecutor executor;

    public IndexRest(final String protocol, final String host, final int port, final HttpExecutor executor) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;
        this.executor = executor;
    }

    public ListenableFuture<IndexResponse> act(IndexRequest request) {
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
            return Futures.transform(executor.post(url.url(), InputStreams.of(request.safeSource())), indexResponseFunction);
        } else {
            return Futures.transform(executor.put(url.seg(request.id()).url(), InputStreams.of(request.safeSource())), indexResponseFunction);
        }
    }

    public static FutureCallback<IndexResponse> indexResponseCallback(final ActionListener<IndexResponse> listener) {
        return new IndexCallback(listener);
    }

    private static Function<HttpResponse, IndexResponse> indexResponseFunction = new Function<HttpResponse, IndexResponse>() {
        @Override public IndexResponse apply(final HttpResponse httpResponse) {
            try {
                //TODO check REST status and "ok" field and handle failure
                Map<String, Object> map = JsonXContent.jsonXContent.createParser(httpResponse.response()).mapAndClose();
                if (map.containsKey("error")) {
                    // FIXME use the right exception
                    throw new RuntimeException("Some kind of error: " + map.toString());
                }
                IndexResponse indexResponse = new IndexResponse(
                    requireString(map.get("_index")),
                    requireString(map.get("_type")),
                    requireString(map.get("_id")),
                    requireLong(map.get("_version")),
                    requireBoolean(map.get("created")));
                return indexResponse;
            } catch (IOException e) {
                // FIXME: which exception to use? It should match ES clients if possible.
                throw new RuntimeException(e);
            }
        }
    };

    private static class IndexCallback implements FutureCallback<IndexResponse> {
        private final ActionListener<IndexResponse> listener;

        private IndexCallback(ActionListener<IndexResponse> listener) {
            this.listener = listener;
        }

        @Override public void onSuccess(final IndexResponse indexResponse) {
            listener.onResponse(indexResponse);
        }

        @Override public void onFailure(final Throwable throwable) {
            // TODO transform failure
            listener.onFailure(throwable);
        }
    }

}
