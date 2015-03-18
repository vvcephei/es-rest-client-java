package org.elasticsearch.action.search;

import com.bazaarvoice.elasticsearch.client.core.spi.HttpResponse;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.io.IOException;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireList;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireMap;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

public class SearchResponseTransform implements Function<HttpResponse, SearchResponse> {
    @Override public SearchResponse apply(final HttpResponse httpResponse) {
        try {
            //TODO check REST status and "ok" field and handle failure
            Map<String, Object> map = JsonXContent.jsonXContent.createParser(httpResponse.response()).mapAndClose();
            if (map.containsKey("error")) {
                // FIXME use the right exception
                throw new RuntimeException("Some kind of error: " + map.toString());
            }

            Map<String, Object> shards = requireMap(map.get("_shards"), String.class, Object.class);
            int totalShards = nodeIntegerValue(shards.get("total"));
            int successfulShards = nodeIntegerValue(shards.get("successful"));
            int failedShards = totalShards - successfulShards;

            InternalFacets facets = null;
            if (map.containsKey("facets")) {
                final Map<String, Object> facetsMap = requireMap(map.get("facets"), String.class, Object.class);
                facets = InternalFacetsHelper.fromXContent(facetsMap);


            }
            return new SearchResponse(
                new InternalSearchResponse(
                    InternalSearchHitsHelper.fromXContent(map),
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
        } catch (IOException e) {
            // FIXME: which exception to use? It should match ES clients if possible.
            throw new RuntimeException(e);
        }
    }


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
}
