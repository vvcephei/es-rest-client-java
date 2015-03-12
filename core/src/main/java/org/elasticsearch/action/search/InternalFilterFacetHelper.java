package org.elasticsearch.action.search;

import org.elasticsearch.search.facet.filter.FilterFacet;
import org.elasticsearch.search.facet.filter.InternalFilterFacet;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class InternalFilterFacetHelper {
    public static FilterFacet fromXContent(final String facetName, final Map<String, Object> facetMap) {
        return new InternalFilterFacet(facetName, nodeLongValue(facetMap.get("count")));
    }
}
