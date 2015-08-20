package org.elasticsearch.search.aggregations.bucket.geogrid;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.action.search.helpers.InternalAggregationsHelper;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

public class GeoHashGridBucketHelper {
    private static final String KEY = "key";
    private static final String DOC_COUNT = "doc_count";
    private static final Set<String> properKeys = ImmutableSet.of(DOC_COUNT,KEY);

    public static InternalGeoHashGrid.Bucket fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final String key = nodeStringValue(map.get(KEY), name);
        final GeoPoint keyAsGeoPoint = GeoHashUtils.decode(key);
        final long keyAsLong = GeoHashUtils.encodeAsLong(keyAsGeoPoint.lat(), keyAsGeoPoint.lon(), key.length());

        final long docCount = nodeLongValue(map.get(DOC_COUNT));

        final Map<String, Object> subAggsMap = Maps.filterKeys(map, new Predicate<String>() {
            @Override public boolean apply(final String s) {
                return !properKeys.contains(s);
            }
        });
        final InternalAggregations aggregations = InternalAggregationsHelper.fromXContentUnwrapped(subAggsMap, manifest);
        return new InternalGeoHashGrid.Bucket(keyAsLong, docCount, aggregations);
    }
}
