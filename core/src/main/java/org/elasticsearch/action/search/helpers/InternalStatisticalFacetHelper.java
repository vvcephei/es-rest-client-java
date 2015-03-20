package org.elasticsearch.action.search.helpers;

import org.elasticsearch.search.facet.statistical.InternalStatisticalFacet;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class InternalStatisticalFacetHelper {
    public static InternalStatisticalFacet fromXContent(final String facetName, final Map<String, Object> facetMap) {

        long count = nodeLongValue(facetMap.get("count"));
        double total = nodeDoubleValue(facetMap.get("total"));
        double min = nodeDoubleValue(facetMap.get("min"));
        double max = nodeDoubleValue(facetMap.get("max"));
        double sum_of_squares = nodeDoubleValue(facetMap.get("sum_of_squares"));
        return new InternalStatisticalFacet(facetName, min, max, total, sum_of_squares, count);
    }
}
