package org.elasticsearch.action.search;

import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet;
import org.elasticsearch.search.facet.filter.FilterFacet;
import org.elasticsearch.search.facet.geodistance.GeoDistanceFacet;
import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.facet.query.QueryFacet;
import org.elasticsearch.search.facet.statistical.StatisticalFacet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.termsstats.InternalTermsStatsFacet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireMap;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireString;

public class InternalFacetsHelper {
    public static InternalFacets fromXContent(final Map<String, Object> content) {
        final List<Facet> facetsList = new ArrayList<Facet>(content.size());
        for (Map.Entry<String, Object> facetEntry : content.entrySet()) {
            final String facetName = facetEntry.getKey();
            final Map<String, Object> facetMap = requireMap(facetEntry.getValue(), String.class, Object.class);
            final String type = requireString(facetMap.get("_type"));
            if (type.equals(DateHistogramFacet.TYPE)) {
                facetsList.add(InternalDateHistogramFacetHelper.fromXContent(facetName, facetMap));
            } else if (type.equals(HistogramFacet.TYPE)) {
                facetsList.add(InternalHistogramFacetHelper.fromXContent(facetName, facetMap));
            } else if (type.equals(TermsFacet.TYPE)) {
                facetsList.add(InternalTermsFacetHelper.fromXContent(facetName, facetMap));
            } else if (type.equals(FilterFacet.TYPE)) {
                facetsList.add(InternalFilterFacetHelper.fromXContent(facetName, facetMap));
            } else if (type.equals(GeoDistanceFacet.TYPE)) {
                facetsList.add(InternalGeoDistanceFacetHelper.fromXContent(facetName, facetMap));
            } else if (type.equals(QueryFacet.TYPE)) {
                facetsList.add(InternalQueryFacetHelper.fromXContent(facetName, facetMap));
            } else if (type.equals("range")) {
                facetsList.add(InternalRangeFacetHelper.fromXContent(facetName, facetMap));
            } else if (type.equals(StatisticalFacet.TYPE)) {
                facetsList.add(InternalStatisticalFacetHelper.fromXContent(facetName, facetMap));
            } else if (type.equals(InternalTermsStatsFacet.TYPE)) {
                facetsList.add(InternalTermsStatsFacetHelper.fromXContent(facetName, facetMap));
            } else {
                throw new IllegalStateException("Unexpected type: " + type);
            }
        }
        return new InternalFacets(facetsList);
    }
}
