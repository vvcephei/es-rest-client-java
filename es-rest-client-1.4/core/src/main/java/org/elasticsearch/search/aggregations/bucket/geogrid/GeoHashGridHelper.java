package org.elasticsearch.search.aggregations.bucket.geogrid;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.filters.*;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersBucketHelper;

import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeListValue;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;

public class GeoHashGridHelper {
    public static final String BUCKETS = "buckets";

    public static InternalAggregation fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final Object bucketsObj = map.get(BUCKETS);
        if (bucketsObj instanceof Map) {
            final Map<String, Object> bucketMap = nodeMapValue(bucketsObj, String.class, Object.class);
            return internalKeyedFilters(name, bucketMap, manifest);
        } else {
            final List<Object> bucketList = nodeListValue(bucketsObj, Object.class);
            return internalAnonFilters(name, bucketList, manifest);
        }
    }

    private static InternalAggregation internalKeyedFilters(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final ImmutableList.Builder<InternalGeoHashGrid.Bucket> buckets = ImmutableList.builder();
        for (Map.Entry<String, Object> bucketObj : map.entrySet()) {
            final String bucketName = bucketObj.getKey();
            final Map<String, Object> bucketMap = nodeMapValue(bucketObj.getValue(), String.class, Object.class);
            buckets.add(GeoHashGridBucketHelper.fromXContent(bucketName, bucketMap, manifest));
        }

        final int requiredSize = -1;
        return new InternalGeoHashGrid(name, requiredSize, buckets.build());
    }

    private static InternalAggregation internalAnonFilters(final String name, final List<Object> list, final AggregationsManifest manifest) {
        final ImmutableList.Builder<InternalGeoHashGrid.Bucket> buckets = ImmutableList.builder();
        for (Object bucketObj : list) {
            final Map<String, Object> bucketMap = nodeMapValue(bucketObj, String.class, Object.class);
            buckets.add(GeoHashGridBucketHelper.fromXContent(null, bucketMap, manifest));
        }
        final int requiredSize = -1;
        return new InternalGeoHashGrid(name, requiredSize, buckets.build());
    }
}
