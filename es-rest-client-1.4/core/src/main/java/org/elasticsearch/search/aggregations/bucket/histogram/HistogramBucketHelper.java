package org.elasticsearch.search.aggregations.bucket.histogram;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.action.search.helpers.InternalAggregationsHelper;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;

public class HistogramBucketHelper {
    private static final String KEY = "key";
    private static final String DOC_COUNT = "doc_count";
    private static final Set<String> properKeys = ImmutableSet.of(DOC_COUNT, KEY);
    public static final ValueFormatter VALUE_FORMATTER = ValueFormatter.RAW;

    public static Histogram.Bucket fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final String key = nodeStringValue(map.get(KEY), name);
        final long docCount = nodeLongValue(map.get(DOC_COUNT));
        final Map<String, Object> subAggsMap = Maps.filterKeys(map, new Predicate<String>() {
            @Override public boolean apply(final String s) {
                return !properKeys.contains(s);
            }
        });
        final InternalAggregations aggregations = InternalAggregationsHelper.fromXContentUnwrapped(subAggsMap, manifest);
        return new InternalHistogram.Bucket(nodeLongValue(key), docCount, VALUE_FORMATTER, aggregations);
    }

    static String numKeyToString(final long key) {
        return VALUE_FORMATTER.format(key);
    }
}
