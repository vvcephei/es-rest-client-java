package com.bazaarvoice.elasticsearch.client.core.util.aggs;

import static org.elasticsearch.common.base.Preconditions.checkState;

public class AggregationManifest {

    private final String type;
    private final AggregationsManifest subAggregationsManifest;

    public AggregationManifest(final String type, final AggregationsManifest subAggregationsManifest) {
        checkState(type != null);
        this.type = type;
        this.subAggregationsManifest = subAggregationsManifest;
    }

    public AggregationsManifest getSubAggregationsManifest() {
        return subAggregationsManifest;
    }

    public String getType() {
        return type;
    }

}
