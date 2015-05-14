package org.elasticsearch.search.aggregations.bucket.range;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.action.search.helpers.InternalAggregationsHelper;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

public class RangeBucketHelper {
    private static final String DOC_COUNT = "doc_count";
    private static final String FROM = "from";
    private static final String FROM_AS_STRING = "from_as_string";
    private static final String TO = "to";
    private static final String TO_AS_STRING = "to_as_string";
    private static final String KEY = "key";
    private static final Set<String> properKeys = ImmutableSet.of(DOC_COUNT, FROM, TO, KEY, FROM_AS_STRING, TO_AS_STRING);

    public static InternalRange.Bucket fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final String key = nodeStringValue(map.get(KEY), name);
        final long docCount = nodeLongValue(map.get(DOC_COUNT));
        final double from = nodeDoubleValue(map.get(FROM), Double.NEGATIVE_INFINITY);
        final double to = nodeDoubleValue(map.get(TO), Double.POSITIVE_INFINITY);

        final Map<String, Object> subAggsMap = Maps.filterKeys(map, new Predicate<String>() {
            @Override public boolean apply(final String s) {
                return !properKeys.contains(s);
            }
        });
        final InternalAggregations aggregations = InternalAggregationsHelper.fromXContentUnwrapped(subAggsMap, manifest);
        return new InternalRange.Bucket(key, from, to, docCount, aggregations, ValueFormatter.RAW);
    }
}
