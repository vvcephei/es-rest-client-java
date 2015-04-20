package org.elasticsearch.action.search.helpers;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationManifest;
import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.TermsHelper;
import org.elasticsearch.search.aggregations.metrics.avg.AvgHelper;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountHelper;

import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;
import static org.elasticsearch.common.base.Preconditions.checkArgument;
import static org.elasticsearch.common.base.Preconditions.checkState;

public class InternalAggregationsHelper {
    public static InternalAggregations fromXContent(final Map<String, Object> map, final AggregationsManifest aggregationsManifest) {
        if (!(map.containsKey("aggregations") || map.containsKey("aggs"))) {
            return new InternalAggregations(ImmutableList.<InternalAggregation>of());
        } else {
            final Map<String, Object> aggregationsMap;
            if (map.containsKey("aggregations")) {
                aggregationsMap = nodeMapValue(map.get("aggregations"), String.class, Object.class);
            } else {
                aggregationsMap = nodeMapValue(map.get("aggs"), String.class, Object.class);
            }

            return fromXContentUnwrapped(aggregationsMap, aggregationsManifest);
        }
    }

    public static InternalAggregations fromXContentUnwrapped(final Map<String, Object> aggregationsMap, final AggregationsManifest aggregationsManifest) {
        if (aggregationsManifest == null) {
            checkArgument(aggregationsMap.isEmpty());
            return new InternalAggregations(ImmutableList.<InternalAggregation>of());
        }
        final ImmutableList.Builder<InternalAggregation> builder = ImmutableList.builder();
        for (Map.Entry<String, AggregationManifest> entry : aggregationsManifest.getManifest().entrySet()) {
            final String name = entry.getKey();
            final String type = entry.getValue().getType();
            checkState(aggregationsMap.containsKey(name));
            final Map<String, Object> subAggregationMap = nodeMapValue(aggregationsMap.get(name), String.class, Object.class);
            final AggregationsManifest subAggregationsManifest = entry.getValue().getSubAggregationsManifest();
            if (type.equals("terms")) {
                builder.add(TermsHelper.fromXContent(name, subAggregationMap, subAggregationsManifest));
            } else if (type.equals("value_count")) {
                builder.add(ValueCountHelper.fromXContent(name, subAggregationMap, subAggregationsManifest));
            } else if (type.equals("avg")) {
                builder.add(AvgHelper.fromXContent(name, subAggregationMap, subAggregationsManifest));
            } else if (type.equals("geohash_grid")) {
                throw new RuntimeException("not implemented");
            } else if (type.equals("date_histogram")) {
                throw new RuntimeException("not implemented");
            } else if (type.equals("histogram")) {
                throw new RuntimeException("not implemented");
            } else if (type.equals("date_range")) {
                throw new RuntimeException("not implemented");
            } else if (type.equals("geo_distance")) {
                throw new RuntimeException("not implemented");
            } else if (type.equals("ip_range")) {
                throw new RuntimeException("not implemented");
            } else if (type.equals("significant_terms")) {
                throw new RuntimeException("not implemented");
            } else if (type.equals("extended_stats")) {
                throw new RuntimeException("not implemented");
            } else {
                throw new IllegalStateException("Unrecognized type: " + type);
            }
        }
        return new InternalAggregations(builder.build());
    }
}
