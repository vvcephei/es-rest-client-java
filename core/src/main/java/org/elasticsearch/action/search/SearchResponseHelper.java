package org.elasticsearch.action.search;

import org.elasticsearch.common.Preconditions;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchShardTarget;

import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireList;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireMap;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

public class SearchResponseHelper {
    public static SearchResponse fromXContent(final Map<String, Object> map) {
        Map<String, Object> shards = requireMap(map.get("_shards"), String.class, Object.class);
        int totalShards = nodeIntegerValue(shards.get("total"));
        int successfulShards = nodeIntegerValue(shards.get("successful"));
        int failedShards = totalShards - successfulShards;

        return new SearchResponse(
            InternalSearchResponseHelper.fromXContent(map),
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