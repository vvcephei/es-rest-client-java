package org.elasticsearch.action.search;

import org.elasticsearch.search.facet.query.InternalQueryFacet;
import org.elasticsearch.search.facet.query.QueryFacet;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class InternalQueryFacetHelper {
    public static QueryFacet fromXContent(final String facetName, final Map<String, Object> facetMap) {
        return new InternalQueryFacet(facetName, nodeLongValue(facetMap.get("count")));
    }
}
