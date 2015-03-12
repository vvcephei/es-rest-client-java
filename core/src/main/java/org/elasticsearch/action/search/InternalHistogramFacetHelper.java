package org.elasticsearch.action.search;

import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.facet.histogram.InternalCountHistogramFacet;
import org.elasticsearch.search.facet.histogram.InternalFullHistogramFacet;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireList;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireMap;
import static org.elasticsearch.common.Preconditions.checkState;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class InternalHistogramFacetHelper {
    public static HistogramFacet fromXContent(final String facetName, final Map<String, Object> facetMap) {
        final List<Object> entries = requireList(facetMap.get("entries"), Object.class);
        InternalCountHistogramFacet.CountEntry[] countEntries = null;
        InternalFullHistogramFacet.FullEntry[] fullEntries = null;
        for (int i = 0; i < entries.size(); i++) {
            final Map<String, Object> entryMap = requireMap(entries.get(i), String.class, Object.class);
            final long key = nodeLongValue(entryMap.get("key"));
            final long count = nodeLongValue(entryMap.get("count"));
            final double min = nodeDoubleValue(entryMap.get("min"), Double.NaN);
            final double max = nodeDoubleValue(entryMap.get("max"), Double.NaN);
            final double total = nodeDoubleValue(entryMap.get("total"), Double.NaN);
            final long totalCount = nodeLongValue(entryMap.get("total_count"), 0);
            final double mean = nodeDoubleValue(entryMap.get("mean"), Double.NaN);
            if (Double.isNaN(min) && Double.isNaN(max) && Double.isNaN(total) && Double.isNaN(mean) && totalCount == 0) {
                checkState(fullEntries == null);
                if (countEntries == null) {
                    countEntries = new InternalCountHistogramFacet.CountEntry[entries.size()];
                }
                countEntries[i] = new InternalCountHistogramFacet.CountEntry(key, count);
            } else {
                checkState(countEntries == null);
                if (fullEntries == null) {
                    fullEntries = new InternalFullHistogramFacet.FullEntry[entries.size()];
                }
                fullEntries[i] = new InternalFullHistogramFacet.FullEntry(key, count, min, max, totalCount, total);
            }
        }
        final HistogramFacet.ComparatorType comparatorType = null; // FIXME not serialized, so there's nothing we can pick here. Not sure of the impact of choosing null.
        if (countEntries != null) {
            return new InternalCountHistogramFacet(facetName, comparatorType, countEntries);
        } else {
            checkState(fullEntries != null);
            return new InternalFullHistogramFacet(facetName, comparatorType, Arrays.asList(fullEntries));
        }
    }
}
