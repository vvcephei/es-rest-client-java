package org.elasticsearch.action.search;

import org.elasticsearch.common.text.StringText;
import org.elasticsearch.search.facet.termsstats.InternalTermsStatsFacet;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacet;
import org.elasticsearch.search.facet.termsstats.doubles.InternalTermsStatsDoubleFacet;
import org.elasticsearch.search.facet.termsstats.longs.InternalTermsStatsLongFacet;
import org.elasticsearch.search.facet.termsstats.strings.InternalTermsStatsStringFacet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireList;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireMap;
import static org.elasticsearch.common.Preconditions.checkState;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class InternalTermsStatsFacetHelper {
    public static InternalTermsStatsFacet fromXContent(final String facetName, final Map<String, Object> facetMap) {
        final TermsStatsFacet.ComparatorType comparatorType = null; // FIXME not serialized, so there's nothing we can pick here. Not sure of the impact of choosing null.
        long missing = nodeLongValue(facetMap.get("missing"));
        Collection<InternalTermsStatsStringFacet.StringEntry> stringEntries = null;
        Collection<InternalTermsStatsDoubleFacet.DoubleEntry> doubleEntries = null;
        Collection<InternalTermsStatsLongFacet.LongEntry> longEntries = null;
        List<Object> terms = requireList(facetMap.get("terms"), Object.class);
        for (Object termEntryO : terms) {
            Map<String, Object> termEntry = requireMap(termEntryO, String.class, Object.class);
            Object termO = termEntry.get("term");
            long count = nodeLongValue(termEntry.get("count"));
            long total_count = nodeLongValue(termEntry.get("total_count"));
            double min = nodeDoubleValue(termEntry.get("min"));
            double max = nodeDoubleValue(termEntry.get("max"));
            double total = nodeDoubleValue(termEntry.get("total"));
            double mean = nodeDoubleValue(termEntry.get("mean"));
            if (termO instanceof Double) {
                if (doubleEntries == null) {doubleEntries = new ArrayList<InternalTermsStatsDoubleFacet.DoubleEntry>(terms.size());}
                doubleEntries.add(new InternalTermsStatsDoubleFacet.DoubleEntry(nodeDoubleValue(termO), count, total_count, total, min, max));
            } else if (termO instanceof String) {
                if (stringEntries == null) {stringEntries = new ArrayList<InternalTermsStatsStringFacet.StringEntry>(terms.size());}
                stringEntries.add(new InternalTermsStatsStringFacet.StringEntry(new StringText((String) termO), count, total_count, total, min, max));
            } else if (termO instanceof Integer || termO instanceof Long) {
                if (longEntries == null) {longEntries = new ArrayList<InternalTermsStatsLongFacet.LongEntry>(terms.size());}
                longEntries.add(new InternalTermsStatsLongFacet.LongEntry(nodeLongValue(termO), count, total_count, total, min, max));
            } else {
                throw new IllegalArgumentException("unexpected facet term type " + termO.getClass() + " term: " + termO);
            }
        }
        if (doubleEntries != null) {
            checkState(stringEntries == null && longEntries == null);
            return new InternalTermsStatsDoubleFacet(facetName, comparatorType, doubleEntries.size(), doubleEntries, missing);
        } else if (stringEntries != null) {
            checkState(longEntries == null);
            return new InternalTermsStatsStringFacet(facetName, comparatorType, stringEntries.size(), stringEntries, missing);
        } else if (longEntries != null) {
            return new InternalTermsStatsLongFacet(facetName, comparatorType, longEntries.size(), longEntries, missing);
        } else {
            checkState(terms.isEmpty());
            // fixme not much we can do but guess here. Fix it by serializing the type of facet.
            return new InternalTermsStatsDoubleFacet(facetName, comparatorType, 0, Collections.<InternalTermsStatsDoubleFacet.DoubleEntry>emptyList(), missing);
        }
    }
}
