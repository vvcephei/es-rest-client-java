package org.elasticsearch.action.search;

import com.bazaarvoice.elasticsearch.client.core.HttpExecutor;
import com.bazaarvoice.elasticsearch.client.core.HttpResponse;
import com.bazaarvoice.elasticsearch.client.core.util.InputStreams;
import com.bazaarvoice.elasticsearch.client.core.util.UrlBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.util.concurrent.FutureCallback;
import org.elasticsearch.common.util.concurrent.Futures;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.readBytesReference;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireList;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireMap;
import static com.bazaarvoice.elasticsearch.client.core.util.Rest.findRestStatus;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.ignoreIndicesToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.scrollToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.searchTypeToString;
import static org.elasticsearch.common.base.Optional.fromNullable;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeFloatValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

public class SearchRest {
    public static ListenableFuture<SearchResponse> act(HttpExecutor executor, SearchRequest request) {
        UrlBuilder url = UrlBuilder.create();

        if (request.indices() == null || request.indices().length == 0) {
            url = url.path("_search");
        } else {
            String indices = Joiner.on(',').skipNulls().join(request.indices());
            if (request.types() == null || request.types().length == 0) {
                url = url.path(indices, "_search");
            } else if (request.types() == null || request.types().length == 0) {
                String types = Joiner.on(',').skipNulls().join(request.types());
                url = url.path(indices, types, "_search");
            }
        }

        if (request.extraSource() != null) {
            throw new NotImplementedException();// TODO: implement. not bothering with this for now...
        }

        url = url
            .paramIfPresent("search_type", fromNullable(request.searchType()).transform(searchTypeToString))
            .paramIfPresent("scroll", fromNullable(request.scroll()).transform(scrollToString))
            .paramIfPresent("routing", fromNullable(request.routing()))
            .paramIfPresent("preference", fromNullable(request.preference()))
            .paramIfPresent("ignore_indices", fromNullable(toNullIfDefault(request.ignoreIndices())).transform(ignoreIndicesToString))
        ;


        return Futures.transform(executor.post(url.url(), InputStreams.of(request.source())), searchResponseFunction);
    }

    private static IgnoreIndices toNullIfDefault(final IgnoreIndices ignoreIndices) {
        // TODO add a string serialization for DEFAULT with a PR, then get rid of this method
        if (IgnoreIndices.DEFAULT.equals(ignoreIndices)) {
            return null;
        } else {
            return ignoreIndices;
        }
    }

    public static FutureCallback<SearchResponse> searchResponseCallback(final ActionListener<SearchResponse> listener) {
        return new SearchCallback(listener);
    }

    private static Function<HttpResponse, SearchResponse> searchResponseFunction = new Function<HttpResponse, SearchResponse>() {
        @Override public SearchResponse apply(final HttpResponse httpResponse) {
            try {
                //TODO check REST status and "ok" field and handle failure
                Map<String, Object> map = JsonXContent.jsonXContent.createParser(httpResponse.response()).mapAndClose();
                Map<String, Object> shards = requireMap(map.get("_shards"), String.class, Object.class);
                int totalShards = nodeIntegerValue(shards.get("total"));
                int successfulShards = nodeIntegerValue(shards.get("successful"));
                int failedShards = totalShards - successfulShards;
                final ShardSearchFailure[] shardSearchFailures = new ShardSearchFailure[failedShards];
                Object[] failures = requireList(shards.get("failures"), Object.class).toArray();
                for (int i = 0; i < failedShards; i++) {
                    Map<String, Object> failure = requireMap(failures[i], String.class, Object.class);
                    SearchShardTarget shard = null;
                    if (failure.containsKey("index") && failure.containsKey("shard")) {
                        String index = nodeStringValue(failure.get("index"), null);
                        Integer shardId = nodeIntegerValue(failure.get("shard"));
                        shard = new SearchShardTarget(null, index, shardId);
                    }

                    shardSearchFailures[i] =
                        new ShardSearchFailure(
                            nodeStringValue(failure.get("reason"), null),
                            shard,
                            findRestStatus(nodeIntegerValue(failure.get("status")))
                        );
                }

                InternalSearchHits hits = null;
                if (map.containsKey("hits")) {
                    Map<String, Object> hitsMap = requireMap(map.get("hits"), String.class, Object.class);
                    long total = nodeLongValue(hitsMap.get("total"));
                    float maxScore = hitsMap.get("max_score") != null ? nodeFloatValue(hitsMap.get("max_score")) : Float.NaN;
                    List<InternalSearchHit> internalSearchHits = Lists.newArrayList();
                    if (hitsMap.containsKey("hits")) {
                        List<Object> hitsList = requireList(hitsMap.get("hits"), Object.class);
                        for (Object hit : hitsList) {
                            Map<String, Object> hitMap = requireMap(hit, String.class, Object.class);
                            Object explanation = hitMap.get("_explanation");
                            String nodeid = null;
                            int shardid = -1; // FIXME not quite right, but the es serialization node is confusing
                            // it only serializes _shard and _node if explanation != null, but it always serializes _index,
                            // and the only way _index could be set is at the same time as the other fields,
                            // (unless it comes from the read(instream) method, in which case shardid and index get set, but nodeid may be unset.
                            // which suggests that at least index and shardid are set on the ES side, but they are deliberately
                            // leaving shardid off from the serialization when explanation() is null.
                            // If so, there is no way I can recover that information, so I'll just set shardId to an impossible value
                            // TODO send a PR to elasticsearch to fix this on the server side
                            if (explanation != null) {
                                shardid = nodeIntegerValue(hitMap.get("_shard"));
                                nodeid = nodeStringValue(hitMap.get("_node"), null);
                            }
                            String index = nodeStringValue(hitMap.get("_index"), null);
                            SearchShardTarget searchShardTarget = new SearchShardTarget(nodeid, index, shardid);
                            String type = nodeStringValue(hitMap.get("_type"), null);
                            String id = nodeStringValue(hitMap.get("_id"), null);
                            long version = nodeLongValue(hitMap.get("_version"), -1);
                            float score = nodeFloatValue(hitMap.get("_score"), Float.NaN);
                            BytesReference source = readBytesReference(hitMap.get("_source"));
                            InternalSearchHit internalSearchHit = new InternalSearchHit(docid, id, type, source, fields);
                            internalSearchHit.shardTarget(searchShardTarget);
                        }
                    }
                }
                SearchResponse searchResponse = new SearchResponse(
                    new InternalSearchResponse(
                        hits,
                        facets,
                        suggest,
                        nodeBooleanValue(map.get("timed_out"))
                    ),
                    nodeStringValue(map.get("_scroll_id"), null),
                    totalShards,
                    successfulShards,
                    nodeLongValue(map.get("took")),
                    shardSearchFailures);
//                SearchResponse indexResponse = new SearchResponse(
//                    requireString(map.get("_index")),
//                    requireString(map.get("_type")),
//                    requireString(map.get("_id")),
//                    requireLong(map.get("_version")));
//                if (map.containsKey("matches")) {
//                    List<String> matches = requireList(map.get("matches"), String.class);
//                    indexResponse.setMatches(matches);
//                }
//                return indexResponse;
            } catch (IOException e) {
                // FIXME: which exception to use? It should match ES clients if possible.
                throw new RuntimeException(e);
            }
        }
    };


    private static class SearchCallback implements FutureCallback<SearchResponse> {
        private final ActionListener<SearchResponse> listener;

        private SearchCallback(ActionListener<SearchResponse> listener) {
            this.listener = listener;
        }

        @Override public void onSuccess(final SearchResponse indexResponse) {
            listener.onResponse(indexResponse);
        }

        @Override public void onFailure(final Throwable throwable) {
            // TODO transform failure
            listener.onFailure(throwable);
        }
    }
}
