package com.bazaarvoice.elasticsearch.client.core;

import org.elasticsearch.action.search.helpers.InternalAggregationsHelper;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.children.Children;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNested;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRange;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.IPv4Range;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;


/**
 * Facade which gives users methods to call to get each type of aggregation.
 * This facade works on any ES {@link org.elasticsearch.client.Client}, but it is *required* for the {@link HttpClient}.
 * <p/>
 * Rather than asking callers to cast aggregations to the required type, we give them methods to retrieve the correct type.
 * <p/>
 * This gives us (in {@link HttpClient}) the ability to switch on the desired type and parse the response map in knowledge
 * of the desired type (which is currently lost through erasure).
 * <p/>
 * But it also has a big discoverability benefit for the user. They may know that they asked for a "double term" aggregation,
 * but the doesn't mean they need to be familiar enough with the ES codebase to select the correct implementation class to cast to.
 * I expect most users simply debug the response to find out the returned type and then hard-code that one, leading to
 * brittle and poorly understood code.
 */
public class TypedAggregations {
    private final Aggregations aggregations;
    private final InternalAggregationsHelper.UnrealizedAggregations unrealizedAggregations;

    public static TypedAggregations wrap(Aggregations aggregations) { return new TypedAggregations(aggregations); }

    private TypedAggregations(final Aggregations aggregations) {
        if (aggregations instanceof InternalAggregationsHelper.UnrealizedAggregations) {
            this.unrealizedAggregations = ((InternalAggregationsHelper.UnrealizedAggregations) aggregations);
            this.aggregations = null;
        } else {
            this.aggregations = aggregations;
            this.unrealizedAggregations = null;
        }
    }

    private boolean isUnrealized() {
        return unrealizedAggregations != null;
    }

    // Terms

    public DoubleTerms getDoubleTerms(final String name) {
        if (isUnrealized()) {
            return unrealizedAggregations.getDoubleTerms(name);
        } else {
            return aggregations.get(name);
        }
    }

    public LongTerms getLongTerms(final String name) {
        if (isUnrealized()) {
            return unrealizedAggregations.getLongTerms(name);
        } else {
            return aggregations.get(name);
        }
    }

    public StringTerms getStringTerms(final String name) {
        if (isUnrealized()) {
            return unrealizedAggregations.getStringTerms(name);
        } else {
            return aggregations.get(name);
        }
    }

    // ValueCount

    public ValueCount getValueCount(final String name) { return null;}


    // Significant

    public SignificantLongTerms getSignificantLongTerms(final String name) {
        return null;
    }

    public SignificantStringTerms getSignificantStringTerms(final String name) {
        return null;
    }

    // Range

    public DateRange getDateRange(final String name) {
        return null;
    }

    public GeoDistance getGeoDistance(final String name) {
        return null;
    }

    public IPv4Range getIPv4Range(final String name) {
        return null;
    }

    // Nested

    public Nested getNested(final String name) {
        return null;
    }

    public ReverseNested getReverseNested(final String name) {
        return null;
    }

    // Missing

    public Missing getMissing(final String name) {
        return null;
    }

    // Histogram

    public DateHistogram getDateHistogram(final String name) {
        return null;
    }

    public Histogram getHistogram(final String name) {
        // This is InternalHistogram.class
        return null;
    }

    // Global

    public Global getGlobal(final String name) {
        return null;
    }

    // GeoGrid

    public GeoHashGrid getGeoHashGrid(final String name) {
        return null;
    }

    // Filters

    public Filters getFilters(final String name) {
        return null;
    }

    // Filter

    public Filter getFilter(final String name) {
        return null;
    }

    // Children

    public Children getChildren(final String name) {
        return null;
    }
}