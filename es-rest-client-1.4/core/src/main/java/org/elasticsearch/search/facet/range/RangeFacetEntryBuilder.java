package org.elasticsearch.search.facet.range;

/** We're located in this class because the constructor is package-protected */
public class RangeFacetEntryBuilder {
    public static RangeFacet.Entry entry(final double from, final String fromAsString, final double to, final String toAsString, final long count, final double total, final long totalCount) {
        final RangeFacet.Entry entry = new RangeFacet.Entry();
        entry.from = from;
        entry.fromAsString = fromAsString;
        entry.to = to;
        entry.toAsString = toAsString;
        entry.count = count;
        entry.total = total;
        entry.totalCount = totalCount;
        return entry;
    }

    public static RangeFacet.Entry entry(final double from, final String fromAsString, final double to, final String toAsString, final double max, final double min, final long count, final double total, final long totalCount) {
        final RangeFacet.Entry entry = new RangeFacet.Entry();
        entry.from = from;
        entry.fromAsString = fromAsString;
        entry.to = to;
        entry.toAsString = toAsString;
        entry.max = max;
        entry.min = min;
        entry.count = count;
        entry.total = total;
        entry.totalCount = totalCount;
        return entry;
    }
}
