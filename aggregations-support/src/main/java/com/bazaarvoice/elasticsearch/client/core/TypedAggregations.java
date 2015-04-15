package com.bazaarvoice.elasticsearch.client.core;

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
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRange;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.IPv4Range;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;


/**
 * Facade which gives users methods to call to get each type of aggregation.
 * <p/>
 * The main benefit for the user is discoverability. They may know that they asked for a "term" aggregation over a value that is a double,
 * but the doesn't mean they need to be familiar enough with the ES codebase to select the correct interface to cast to.
 * <p/>
 * I expect most users simply debug the response to find out the returned type and then hard-code that one, leading to
 * brittle and poorly understood code, since the debugger will tell you that the implementation class, not the interface.
 */
public class TypedAggregations {
    private final Aggregations aggregations;

    private TypedAggregations(final Aggregations aggregations) {
        this.aggregations = aggregations;
    }

    public static TypedAggregations typed(Aggregations aggregations) { return new TypedAggregations(aggregations); }

    // Terms

    public Terms getTerms(final String name) {
        return aggregations.get(name);
    }

    // Stats

    public ValueCount getValueCount(final String name) {
        return aggregations.get(name);
    }

    public Avg getAvg(final String name) {
        return aggregations.get(name);
    }

    public Min getMin(final String name) {
        return aggregations.get(name);
    }

    public Max getMax(final String name) {
        return aggregations.get(name);
    }

    public Sum getSum(final String name) {
        return aggregations.get(name);
    }

    public Stats getStats(final String name) {
        return aggregations.get(name);
    }

    public ExtendedStats getExtendedStats(final String name) {
        return aggregations.get(name);
    }

    // Percentiles


    public Percentiles getPercentiles(final String name) {
        return aggregations.get(name);
    }


    public PercentileRanks getPercentileRanks(final String name) {
        return aggregations.get(name);
    }

    // Cardinality

    public Cardinality getCardinality(final String name) {
        return aggregations.get(name);
    }

    // GeoBounds

    public GeoBounds getGeoBounds(final String name) {
        return aggregations.get(name);
    }

    // TopHits

    public TopHits getTopHits(final String name) { return aggregations.get(name); }

    // ScriptedMetric

    public ScriptedMetric getScriptedMetric(final String name) { return aggregations.get(name); }

    // Significant

    public SignificantTerms getSignificantTerms(final String name) {
        return aggregations.get(name);
    }

    // Range

    public Range getRange(final String name) {
        return aggregations.get(name);
    }

    public DateRange getDateRange(final String name) {
        return aggregations.get(name);
    }

    public GeoDistance getGeoDistance(final String name) {
        return aggregations.get(name);
    }

    public IPv4Range getIPv4Range(final String name) {
        return aggregations.get(name);
    }


    // Histogram

    public DateHistogram getDateHistogram(final String name) {
        return aggregations.get(name);
    }

    public Histogram getHistogram(final String name) {
        // This is InternalHistogram.class
        return aggregations.get(name);
    }

    // GeoGrid

    public GeoHashGrid getGeoHashGrid(final String name) {
        return aggregations.get(name);
    }

    // ???????????????????????????

    // Nested

    public Nested getNested(final String name) {
        return aggregations.get(name);
    }

    public ReverseNested getReverseNested(final String name) {
        return aggregations.get(name);
    }

    // Missing

    public Missing getMissing(final String name) {
        return aggregations.get(name);
    }


    // Global

    public Global getGlobal(final String name) {
        return aggregations.get(name);
    }


    // Filters

    public Filters getFilters(final String name) {
        return aggregations.get(name);
    }

    // Filter

    public Filter getFilter(final String name) {
        return aggregations.get(name);
    }

    // Children

    public Children getChildren(final String name) {
        return aggregations.get(name);
    }

}
