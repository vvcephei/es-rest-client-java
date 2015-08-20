package org.elasticsearch.search.aggregations.metrics.stats.extended;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class ExtendedStatsHelper {
    public static InternalAggregation fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final long count = nodeLongValue(map.get("count"));
        final double sum = nodeDoubleValue(map.get("sum"));
        final double min = nodeDoubleValue(map.get("min"));
        final double max = nodeDoubleValue(map.get("max"));
        final double sumOfSqrs = nodeDoubleValue(map.get("sum_of_squares"));
        return new InternalExtendedStats(name, count, sum, min, max, sumOfSqrs);
    }
}
