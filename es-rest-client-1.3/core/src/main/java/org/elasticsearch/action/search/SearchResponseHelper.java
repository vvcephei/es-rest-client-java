package org.elasticsearch.action.search;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.action.FromXContent;
import org.elasticsearch.action.search.helpers.InternalSearchResponseHelper;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchShardTarget;

import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.toMap;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeListValue;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;
import static org.elasticsearch.common.base.Preconditions.checkState;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

/**
 * The inverse of {@link org.elasticsearch.action.search.SearchResponse#toXContent(XContentBuilder, Params)}
 */
public class SearchResponseHelper implements FromXContent<SearchResponse> {
    final AggregationsManifest aggregationsManifest;

    public SearchResponseHelper(final SearchRequest request) {
        final Map<String, Object> source = toMap(request.source());
        if (source == null) {
            aggregationsManifest = null;
        } else if (source.containsKey("aggregations")) {
            aggregationsManifest = AggregationsManifest.fromSource(nodeMapValue(source.get("aggregations"), String.class, Object.class));
        } else if (source.containsKey("aggs")) {
            aggregationsManifest = AggregationsManifest.fromSource(nodeMapValue(source.get("aggs"), String.class, Object.class));
        } else {
            aggregationsManifest = null;
        }
    }

    @Override public SearchResponse fromXContent(final Map<String, Object> map) {
        Map<String, Object> shards = nodeMapValue(map.get("_shards"), String.class, Object.class);
        int totalShards = nodeIntegerValue(shards.get("total"));
        int successfulShards = nodeIntegerValue(shards.get("successful"));
        int failedShards = totalShards - successfulShards;

        return new SearchResponse(
            InternalSearchResponseHelper.fromXContent(map, aggregationsManifest),
            nodeStringValue(map.get("_scroll_id"), null),
            totalShards,
            successfulShards,
            nodeLongValue(map.get("took")),
            getShardSearchFailures(shards, failedShards));
    }

    private static ShardSearchFailure[] getShardSearchFailures(final Map<String, Object> shards, final int failedShards) {
        final ShardSearchFailure[] shardSearchFailures = new ShardSearchFailure[failedShards];
        if (failedShards == 0) {
            Preconditions.checkState(!shards.containsKey("failures"));
            return shardSearchFailures;
        }
        Object[] failures = nodeListValue(shards.get("failures"), Object.class).toArray();
        for (int i = 0; i < failedShards; i++) {
            Map<String, Object> failure = nodeMapValue(failures[i], String.class, Object.class);
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
