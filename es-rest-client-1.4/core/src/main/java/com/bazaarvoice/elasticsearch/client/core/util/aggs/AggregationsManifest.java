package com.bazaarvoice.elasticsearch.client.core.util.aggs;

import org.elasticsearch.common.collect.ImmutableMap;

import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;
import static org.elasticsearch.common.base.Preconditions.checkState;

public class AggregationsManifest {

    final ImmutableMap<String, AggregationManifest> manifest;

    private AggregationsManifest(final ImmutableMap<String, AggregationManifest> manifest) {this.manifest = manifest;}

    public static AggregationsManifest fromSource(Map<String, Object> unwrappedAggregationsSource) {
        ImmutableMap.Builder<String, AggregationManifest> builder = ImmutableMap.builder();
        for (Map.Entry<String, Object> entry : unwrappedAggregationsSource.entrySet()) {
            final String name = entry.getKey();
            final Map<String, Object> aggregation = nodeMapValue(entry.getValue(), String.class, Object.class);
            AggregationsManifest subAggManifest = null;
            String type = null;
            for (Map.Entry<String, Object> field : aggregation.entrySet()) {
                if (field.getKey().equals("aggregations") || field.getKey().equals("aggs")) {
                    subAggManifest = AggregationsManifest.fromSource(nodeMapValue(field.getValue(), String.class, Object.class));
                } else {
                    // assume this is the "type" of the agg; we should only see one.
                    checkState(type == null);
                    type = field.getKey();
                }
            }
            builder.put(name, new AggregationManifest(type, subAggManifest));
        }
        return new AggregationsManifest(builder.build());
    }

    public ImmutableMap<String, AggregationManifest> getManifest() {
        return manifest;
    }
}
