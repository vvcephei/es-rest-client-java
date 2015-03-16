package org.elasticsearch.action.search;

import com.bazaarvoice.elasticsearch.client.core.spi.HttpExecutor;
import com.bazaarvoice.elasticsearch.client.core.spi.HttpResponse;
import com.bazaarvoice.elasticsearch.client.core.util.InputStreams;
import com.bazaarvoice.elasticsearch.client.core.util.UrlBuilder;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.AbstractRestClientAction;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.concurrent.FutureCallback;
import org.elasticsearch.common.util.concurrent.Futures;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.readBytesReference;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireList;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireMap;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.booleanToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.scrollToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.searchTypeToString;
import static org.elasticsearch.common.base.Optional.fromNullable;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeFloatValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

public class SearchRest extends AbstractRestClientAction<SearchRequest, SearchResponse> {
    public SearchRest(final String protocol, final String host, final int port, final HttpExecutor executor) {
        super(protocol, host, port, executor);
    }

    @Override public ListenableFuture<SearchResponse> act(final SearchRequest request) {
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

        if (request.templateSource() != null) {
            throw new NotImplementedException();// TODO: implement. not bothering with this for now...
        }

        if (request.extraSource() != null) {
            throw new NotImplementedException();// TODO: implement. not bothering with this for now...
        }

        url = url
            .paramIfPresent("search_type", fromNullable(request.searchType()).transform(searchTypeToString))
            .paramIfPresent("query_cache", fromNullable(request.queryCache()).transform(booleanToString))
            .paramIfPresent("scroll", fromNullable(request.scroll()).transform(scrollToString))
            .paramIfPresent("routing", fromNullable(request.routing()))
            .paramIfPresent("preference", fromNullable(request.preference()))
        ;


        return Futures.transform(executor.post(url.url(), InputStreams.of(request.source())), searchResponseFunction);
    }

    @Override public FutureCallback<SearchResponse> callback(final ActionListener<SearchResponse> listener) {
        return new SearchCallback(listener);
    }

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

    private static Function<HttpResponse, SearchResponse> searchResponseFunction = new Function<HttpResponse, SearchResponse>() {
        @Override public SearchResponse apply(final HttpResponse httpResponse) {
            try {
                //TODO check REST status and "ok" field and handle failure
                Map<String, Object> map = JsonXContent.jsonXContent.createParser(httpResponse.response()).mapAndClose();
                Map<String, Object> shards = requireMap(map.get("_shards"), String.class, Object.class);
                int totalShards = nodeIntegerValue(shards.get("total"));
                int successfulShards = nodeIntegerValue(shards.get("successful"));
                int failedShards = totalShards - successfulShards;

                InternalFacets facets = null;
                if (map.containsKey("facets")) {
                    final Map<String, Object> facetsMap = requireMap(map.get("facets"), String.class, Object.class);
                    facets = InternalFacetsHelper.fromXContent(facetsMap);


                }
                SearchResponse searchResponse = new SearchResponse(
                    new InternalSearchResponse(
                        getSearchHits(map),
                        facets,
                        null,// TODO aggregations
                        null, // TODO suggest
                        nodeBooleanValue(map.get("timed_out")),
                        nodeBooleanValue(map.get("terminated_early"))
                    ),
                    nodeStringValue(map.get("_scroll_id"), null),
                    totalShards,
                    successfulShards,
                    nodeLongValue(map.get("took")),
                    getShardSearchFailures(shards, failedShards));
                return searchResponse;
            } catch (IOException e) {
                // FIXME: which exception to use? It should match ES clients if possible.
                throw new RuntimeException(e);
            }
        }
    };

    private static ShardSearchFailure[] getShardSearchFailures(final Map<String, Object> shards, final int failedShards) {
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
        return shardSearchFailures;
    }

    private static RestStatus findRestStatus(final int status) {
        for (RestStatus restStatus : RestStatus.values()) {
            if (restStatus.getStatus() == status) {
                return restStatus;
            }
        }
        throw new IllegalArgumentException("invalid status: " + status);
    }

    private static InternalSearchHits getSearchHits(final Map<String, Object> map) {
        InternalSearchHits hits = null;
        if (map.containsKey("hits")) {
            Map<String, Object> hitsMap = requireMap(map.get("hits"), String.class, Object.class);
            final long totalHits = nodeLongValue(hitsMap.get("total"));
            final float maxScore = hitsMap.get("max_score") != null ? nodeFloatValue(hitsMap.get("max_score")) : Float.NaN;

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
                    Text type = new StringText(nodeStringValue(hitMap.get("_type"), null));
                    String id = nodeStringValue(hitMap.get("_id"), null);
                    long version = nodeLongValue(hitMap.get("_version"), -1);
                    float score = nodeFloatValue(hitMap.get("_score"), Float.NaN);
                    BytesReference source = readBytesReference(hitMap.get("_source"));
                    final int docId = -1; // this field isn't serialized
                    ImmutableMap.Builder<String, SearchHitField> fields = ImmutableMap.builder();
                    if (hitMap.containsKey("fields")) {
                        Map<String, Object> fieldsMap = requireMap(hitMap.get("fields"), String.class, Object.class);
                        for (Map.Entry<String, Object> fieldEntry : fieldsMap.entrySet()) {
                            final ImmutableList.Builder<Object> valuesBuilder = ImmutableList.builder();
                            if (fieldEntry.getValue() instanceof List) {
                                for (Object value : requireList(fieldEntry.getValue(), Object.class)) {
                                    valuesBuilder.add(value);
                                }
                            } else {
                                valuesBuilder.add(fieldEntry.getValue());
                            }
                            SearchHitField field = new InternalSearchHitField(fieldEntry.getKey(), valuesBuilder.build());
                            fields.put(field.getName(), field);
                        }
                    }
                    InternalSearchHit internalSearchHit = new InternalSearchHit(docId, id, type, fields.build());
                    internalSearchHit.sourceRef(source);

                    internalSearchHit.shardTarget(searchShardTarget);

                    if (hitMap.containsKey("highlight")) {
                        ImmutableMap.Builder<String, HighlightField> highlights = ImmutableMap.builder();
                        final Map<String, Object> highlightMap = requireMap(hitMap.get("highlight"), String.class, Object.class);
                        for (Map.Entry<String, Object> entry : highlightMap.entrySet()) {
                            final String name = entry.getKey();
                            final Text[] fragments;
                            if (entry.getValue() == null) {
                                fragments = null;
                            } else {
                                final List<String> strings = requireList(entry.getValue(), String.class);
                                fragments = new Text[strings.size()];
                                for (int i = 0; i < strings.size(); i++) {
                                    fragments[i] = new StringText(strings.get(i));
                                }
                            }
                            final HighlightField highlightField = new HighlightField(name, fragments);
                            highlights.put(highlightField.getName(), highlightField);
                        }
                        internalSearchHit.highlightFields(highlights.build());
                    }

                    if (hitMap.containsKey("sort")) {
                        // TODO: can't really tell if this is the right thing to do.
                        final List<Object> sorts = requireList(hitMap.get("sort"), Object.class);
                        internalSearchHit.sortValues(sorts.toArray());
                    }

                    if (hitMap.containsKey("matched_filters")) {
                        final List<String> matched_filters = requireList(hitMap.get("matched_filters"), String.class);
                        internalSearchHit.matchedQueries(matched_filters.toArray(new String[matched_filters.size()]));
                    }

                    if (explanation != null) {
                        internalSearchHit.explanation(getExplanation(explanation));
                    }

                    internalSearchHits.add(internalSearchHit);
                }
            }
            hits = new InternalSearchHits(internalSearchHits.toArray(new InternalSearchHit[internalSearchHits.size()]), totalHits, maxScore);
        }
        return hits;
    }

    private static Explanation getExplanation(final Object explanationObj) {
        final Map<String, Object> explainMap = requireMap(explanationObj, String.class, Object.class);
        final float value = nodeFloatValue(explainMap.get("value"));
        final String description = nodeStringValue(explainMap.get("description"), null);
        final Explanation explanation = new Explanation(value, description);
        if (explainMap.containsKey("details")) {
            for (Object detail : requireList(explainMap.get("details"), Object.class)) {
                explanation.addDetail(getExplanation(detail));
            }
        }
        return explanation;
    }
}
