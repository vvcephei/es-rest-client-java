package org.elasticsearch.action.search.helpers;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.suggest.Suggest;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

public class InternalSearchResponseHelper {
    public static InternalSearchResponse fromXContent(final Map<String, Object> map, final AggregationsManifest aggregationsManifest) {

        final InternalSearchHits searchHits = InternalSearchHitsHelper.fromXContent(map);
        final InternalFacets facets = InternalFacetsHelper.fromXContent(map);
        final InternalAggregations aggregations = InternalAggregationsHelper.fromXContent(map, aggregationsManifest);
        final Suggest suggest = SuggestHelper.fromXContent(map);
        final boolean timed_out = nodeBooleanValue(map.get("timed_out"));
        final boolean terminated_early = nodeBooleanValue(map.get("terminated_early"), false);
        return new InternalSearchResponse(
            searchHits,
            facets,
            aggregations,
            suggest,
            timed_out,
            terminated_early
        );
    }
}
