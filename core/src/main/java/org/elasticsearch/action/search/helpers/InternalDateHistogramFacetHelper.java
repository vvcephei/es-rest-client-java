package org.elasticsearch.action.search.helpers;

import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet;
import org.elasticsearch.search.facet.datehistogram.InternalCountDateHistogramFacet;
import org.elasticsearch.search.facet.datehistogram.InternalDateHistogramFacet;
import org.elasticsearch.search.facet.datehistogram.InternalFullDateHistogramFacet;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeListValue;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;
import static org.elasticsearch.common.Preconditions.checkState;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class InternalDateHistogramFacetHelper {
    public static  InternalDateHistogramFacet fromXContent(final String facetName, final Map<String,Object> facetMap){
        final List<Object> entries = nodeListValue(facetMap.get("entries"), Object.class);
        InternalCountDateHistogramFacet.CountEntry[] countEntries = null;
        InternalFullDateHistogramFacet.FullEntry[] fullEntries = null;
        for (int i = 0; i < entries.size(); i++) {
            final Map<String, Object> entryMap = nodeMapValue(entries.get(i), String.class, Object.class);
            final long time = nodeLongValue(entryMap.get("time"));
            final long count = nodeLongValue(entryMap.get("count"));
            final double min = nodeDoubleValue(entryMap.get("min"), Double.NaN);
            final double max = nodeDoubleValue(entryMap.get("max"), Double.NaN);
            final double total = nodeDoubleValue(entryMap.get("total"), Double.NaN);
            final long totalCount = nodeLongValue(entryMap.get("total_count"), 0);
            final double mean = nodeDoubleValue(entryMap.get("mean"), Double.NaN);
            if (Double.isNaN(min) && Double.isNaN(max) && Double.isNaN(total) && Double.isNaN(mean) && totalCount == 0) {
                checkState(fullEntries == null);
                if (countEntries == null) {
                    countEntries = new InternalCountDateHistogramFacet.CountEntry[entries.size()];
                }
                countEntries[i] = new InternalCountDateHistogramFacet.CountEntry(time, count);
            } else {
                checkState(countEntries == null);
                if (fullEntries == null) {
                    fullEntries = new InternalFullDateHistogramFacet.FullEntry[entries.size()];
                }
                fullEntries[i] = new InternalFullDateHistogramFacet.FullEntry(time, count, min, max, totalCount, total);
            }
        }
        // FIXME TO_PR not serialized, so there's nothing we can pick here. Not sure of the impact of choosing null. see https://github.com/bazaarvoice/es-client-java/issues/7
        final DateHistogramFacet.ComparatorType comparatorType = null;
        InternalDateHistogramFacet facet;
        if (countEntries != null) {
            return new InternalCountDateHistogramFacet(facetName, comparatorType, countEntries);
        } else {
            checkState(fullEntries != null);
            return new InternalFullDateHistogramFacet(facetName, comparatorType, Arrays.asList(fullEntries));
        }
    }
}
