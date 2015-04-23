package org.elasticsearch.search.aggregations.metrics.tophits;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.action.search.helpers.InternalSearchHitsHelper;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.internal.InternalSearchHits;

import java.util.Map;

public class TopHitsHelper {
    public static InternalAggregation fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final InternalSearchHits searchHits = InternalSearchHitsHelper.fromXContent(map);
        return new InternalTopHits(name, searchHits);
    }
}
