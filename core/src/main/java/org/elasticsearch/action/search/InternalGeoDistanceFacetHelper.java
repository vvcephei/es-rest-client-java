package org.elasticsearch.action.search;

import org.elasticsearch.search.facet.geodistance.GeoDistanceFacet;
import org.elasticsearch.search.facet.geodistance.GeoDistanceFacet.Entry;
import org.elasticsearch.search.facet.geodistance.InternalGeoDistanceFacet;

import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireList;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireMap;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class InternalGeoDistanceFacetHelper {
    public static GeoDistanceFacet fromXContent(final String facetName, final Map<String, Object> facetMap) {
        List<Object> ranges = requireList(facetMap.get("ranges"), Object.class);
        Entry[] entries = new Entry[ranges.size()];
        int i = 0;
        for (Object range : ranges) {
            Map<String, Object> rangeMap = requireMap(range, String.class, Object.class);
            double from = nodeDoubleValue(rangeMap.get("from"), Double.POSITIVE_INFINITY);
            double to = nodeDoubleValue(rangeMap.get("to"), Double.POSITIVE_INFINITY);
            long count = nodeLongValue(rangeMap.get("count"));
            double min = nodeDoubleValue(rangeMap.get("min"));
            double max = nodeDoubleValue(rangeMap.get("max"));
            long totalCount = nodeLongValue(rangeMap.get("total_count"));
            double total = nodeDoubleValue(rangeMap.get("total"));
            entries[i++] = new Entry(from, to, count, totalCount, total, min, max);
        }
        return new InternalGeoDistanceFacet(facetName, entries);
    }
}
