package org.elasticsearch.action.search;

import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.search.facet.range.InternalRangeFacet;
import org.elasticsearch.search.facet.range.RangeFacet;

import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireList;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireMap;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;

public class InternalRangeFacetHelper {
    public static RangeFacet fromXContent(final String facetName, final Map<String, Object> facetMap) {
        List<Object> ranges = requireList(facetMap.get("ranges"), Object.class);
        RangeFacet.Entry[] entries = new RangeFacet.Entry[ranges.size()];
        int i = 0;
        for (Object rangeObj : ranges) {
            Map<String, Object> range = requireMap(rangeObj, String.class, Object.class);
            nodeDoubleValue(range.get("from"), Double.POSITIVE_INFINITY);
            
        }
        return new InternalRangeFacet()
    }
}
