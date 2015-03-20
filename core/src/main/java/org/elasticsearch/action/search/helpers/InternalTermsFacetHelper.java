package org.elasticsearch.action.search.helpers;

import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.doubles.InternalDoubleTermsFacet;
import org.elasticsearch.search.facet.terms.longs.InternalLongTermsFacet;
import org.elasticsearch.search.facet.terms.strings.InternalStringTermsFacet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeListValue;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;
import static org.elasticsearch.common.Preconditions.checkNotNull;
import static org.elasticsearch.common.Preconditions.checkState;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeIntegerValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class InternalTermsFacetHelper {
    public static TermsFacet fromXContent(final String facetName, final Map<String, Object> facetMap) {
        // FIXME not serialized, so there's nothing we can pick here. Not sure of the impact of choosing null. see https://github.com/bazaarvoice/es-client-java/issues/9
        final TermsFacet.ComparatorType comparatorType = null;
        final long missing = nodeLongValue(facetMap.get("missing"));
        final long total = nodeLongValue(facetMap.get("total"));
        final long otherCount = nodeLongValue(facetMap.get("other"));
        Collection<InternalDoubleTermsFacet.DoubleEntry> doubleEntries = null;
        Collection<InternalStringTermsFacet.TermEntry> stringEntries = null;
        Collection<InternalLongTermsFacet.LongEntry> longEntries = null;
        final List<Object> terms = nodeListValue(facetMap.get("terms"), Object.class);
        for (Object term : terms) {
            final Map<String, Object> termMap = nodeMapValue(term, String.class, Object.class);
            final int count = nodeIntegerValue(termMap.get("count"));
            final Object actualTerm = termMap.get("term");
            checkNotNull(actualTerm);
            if (actualTerm instanceof Double) {
                if (doubleEntries == null) { doubleEntries = new ArrayList<InternalDoubleTermsFacet.DoubleEntry>(terms.size());}
                doubleEntries.add(new InternalDoubleTermsFacet.DoubleEntry((Double) actualTerm, count));
            } else if (actualTerm instanceof String) {
                if (stringEntries == null) { stringEntries = new ArrayList<InternalStringTermsFacet.TermEntry>(terms.size());}
                stringEntries.add(new InternalStringTermsFacet.TermEntry((String) actualTerm, count));
            } else if (actualTerm instanceof Long || actualTerm instanceof Integer) {
                if (longEntries == null) {longEntries = new ArrayList<InternalLongTermsFacet.LongEntry>(terms.size());}
                longEntries.add(new InternalLongTermsFacet.LongEntry(nodeLongValue(actualTerm), count));
            } else {
                throw new IllegalArgumentException("unexpected facet term type " + actualTerm.getClass() + " term: " + actualTerm);
            }
        }

        if (doubleEntries != null) {
            checkState(stringEntries == null && longEntries == null);
            return new InternalDoubleTermsFacet(facetName, comparatorType, doubleEntries.size(), doubleEntries, missing, total);
        } else if (stringEntries != null) {
            checkState(longEntries == null);
            return new InternalStringTermsFacet(facetName, comparatorType, stringEntries.size(), stringEntries, missing, total);
        } else if (longEntries != null) {
            return new InternalLongTermsFacet(facetName, comparatorType, longEntries.size(), longEntries, missing, total);
        } else {
            checkState(terms.isEmpty());
            // fixme not much we can do but guess here. Fix it by serializing the type of facet.
            return new InternalDoubleTermsFacet(facetName, comparatorType, 0, new ArrayList<InternalDoubleTermsFacet.DoubleEntry>(0), missing, total);
        }
    }
}
