package org.elasticsearch.action.index;

import com.bazaarvoice.elasticsearch.client.core.HttpExecutor;
import com.bazaarvoice.elasticsearch.client.core.HttpResponse;
import com.bazaarvoice.elasticsearch.client.core.util.InputStreams;
import com.bazaarvoice.elasticsearch.client.core.util.UrlBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureCallback;
import org.elasticsearch.common.util.concurrent.Futures;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireList;
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
    public static ListenableFuture<IndexResponse> act(HttpExecutor executor, IndexRequest request) {
        UrlBuilder url = UrlBuilder.create()
            .path(notNull(request.index())).seg(notNull(request.type()))
            .paramIfPresent("routing", fromNullable(request.routing()))
            .paramIfPresent("parent", fromNullable(request.parent()))
            .paramIfPresent("timestamp", fromNullable(request.timestamp()))
            .paramIfPresent("ttl", (request.ttl() == -1) ? Optional.<String>absent() : of(TimeValue.timeValueMillis(request.ttl()).format()))
            .paramIfPresent("timeout", fromNullable(request.timeout()).transform(timeValueToString))
            .paramIfPresent("refresh", fromNullable(request.refresh()).transform(booleanToString))
            .paramIfPresent("version", fromNullable(request.version()).transform(longToString))
            .paramIfPresent("version_type", fromNullable(request.versionType()).transform(versionTypeToString))
            .paramIfPresent("percolate", fromNullable(request.percolate()))
            .paramIfPresent("op_type", fromNullable(request.opType()).transform(opTypeToString))
            .paramIfPresent("replication", fromNullable(request.replicationType()).transform(replicationTypeToString))
            .paramIfPresent("consistency", fromNullable(request.consistencyLevel()).transform(writeConsistencyLevelToString));

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
                IndexResponse indexResponse = new IndexResponse(
                    requireString(map.get("_index")),
                    requireString(map.get("_type")),
                    requireString(map.get("_id")),
                    requireLong(map.get("_version")));
                if (map.containsKey("matches")) {
                    List<String> matches = requireList(map.get("matches"), String.class);
                    indexResponse.setMatches(matches);
                }
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
