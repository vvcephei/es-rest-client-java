package org.elasticsearch.search.aggregations.metrics.stats.extended;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;

import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class ExtendedStatsHelper {
    public static InternalAggregation fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final long count = nodeLongValue(map.get("count"));
        final double sum = nodeDoubleValue(map.get("sum"));
        final double min = nodeDoubleValue(map.get("min"));
        final double max = nodeDoubleValue(map.get("max"));
        final double sumOfSqrs = nodeDoubleValue(map.get("sum_of_squares"));
        final Map<String, Object> std_deviation_bounds = nodeMapValue(map.get("std_deviation_bounds"), String.class, Object.class);
        // TODO ES could serialized sigma and spare us this calculation. I don't think there's a reason to hide it.
        // stdDevUpper = getAvg() + (getStdDeviation() * sigma)
        final double upper = nodeDoubleValue(std_deviation_bounds.get("upper"));
        final double avg = sum / count;
        final double variance = (sumOfSqrs - ((sum * sum) / count)) / count;
        final double stdDev = Math.sqrt(variance);
        final double sigma = (upper - avg) / stdDev;
        return new InternalExtendedStats(name, count, sum, min, max, sumOfSqrs, sigma);
    }
}
