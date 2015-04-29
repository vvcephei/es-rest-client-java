package com.bazaarvoice.elasticsearch.client;

import com.bazaarvoice.elasticsearch.client.core.TypedAggregations;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.children.Children;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNested;
import org.elasticsearch.search.aggregations.bucket.range.Range;
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
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.bazaarvoice.elasticsearch.client.core.TypedAggregations.typed;
import static org.elasticsearch.common.Preconditions.checkState;
import static org.elasticsearch.search.aggregations.AggregationBuilders.reverseNested;
import static org.elasticsearch.search.aggregations.AggregationBuilders.significantTerms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.junit.Assert.fail;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class SearchTest extends JerseyRestClientTest {

    public static final String INDEX = "search-test-idx";
    public static final String TYPE = "search-test-type";
    public static final String TYPE2 = "search-child-type";
    public static final String ID1 = "search-test-id-1";
    public static final String ID2 = "search-test-id-2";

    public static final String SignificantTermsIndex = "search-test-idx-significant-terms";
    public static final String SignificantTermsType = "search-test-type-significant-terms";
    public static final String SignificantTermsIdPrefix = "search-test-id-";
    private final String metroPolice = "Metro Police";
    private final String transportPolice = "British Transport Police";
    private final String robbery = "Robbery";
    private final String bikeTheft = "Bicycle Theft";

    @BeforeClass
    public void setupSearchTest() throws IOException {
        nodeClient().admin().indices().prepareCreate(INDEX).addMapping(TYPE,
            ImmutableMap.<String, Object>of(
                "properties", map(
                    "field", map("type", "string"),
                    "(id)", map("type", "string", "index", "not_analyzed"),
                    "dfield", map("type", "double"),
                    "ifield", map("type", "long"),
                    "location", map("type", "geo_point"),
                    "things", map("type", "nested", "properties", map("field", map("type", "string")))
                ))
        ).addMapping(TYPE2,
            ImmutableMap.<String, Object>of(
                "_parent", ImmutableMap.of("type", TYPE),
                "properties", map(
                    "anotherfield", map("type", "string"),
                    "(id)", map("type", "string", "index", "not_analyzed")
                ))
        ).execute().actionGet();

        restClient().prepareIndex(INDEX, TYPE, ID1).setSource(
            "field", "value",
            "dfield", 2.3,
            "ifield", 4,
            "location", "41.12,-71.34",
            "(id)", ID1,
            "things", ImmutableList.of(ImmutableMap.of("field", "nested"))
        ).setRefresh(true).execute().actionGet();

        restClient().prepareIndex(INDEX, TYPE2, ID2).setSource(
            "anotherfield", "anothervalue",
            "(id)", ID2
        ).setParent(ID1).setRefresh(true).execute().actionGet();


        nodeClient().admin().indices().prepareCreate(SignificantTermsIndex).addMapping(SignificantTermsType,
            ImmutableMap.<String, Object>of(
                "properties", map(
                    "force", map("type", "string", "index", "not_analyzed"),
                    "crime_type", map("type", "string", "index", "not_analyzed")
                )
            )
        ).execute().actionGet();

        for (int i = 0; i < 20; i++) {
            restClient().prepareIndex(SignificantTermsIndex, SignificantTermsType, SignificantTermsIdPrefix + i).setSource(
                "force", metroPolice,
                "crime_type", robbery
            ).execute().actionGet();
        }
        for (int i = 20; i < 30; i++) {
            restClient().prepareIndex(SignificantTermsIndex, SignificantTermsType, SignificantTermsIdPrefix + i).setSource(
                "force", transportPolice,
                "crime_type", bikeTheft
            ).execute().actionGet();
        }
        restClient().prepareIndex(SignificantTermsIndex, SignificantTermsType, SignificantTermsIdPrefix + 30).setSource(
            "force", metroPolice,
            "crime_type", bikeTheft
        ).execute().actionGet();
        restClient().prepareIndex(SignificantTermsIndex, SignificantTermsType, SignificantTermsIdPrefix + 31).setSource(
            "force", transportPolice,
            "crime_type", robbery
        ).execute().actionGet();
        restClient().prepareIndex(SignificantTermsIndex, SignificantTermsType, SignificantTermsIdPrefix + 32).setSource(
            "force", "APD",
            "crime_type", bikeTheft
        ).execute().actionGet();
        restClient().prepareIndex(SignificantTermsIndex, SignificantTermsType, SignificantTermsIdPrefix + 33).setSource(
            "force", "APD",
            "crime_type", robbery
        ).execute().actionGet();

        nodeClient().admin().indices().prepareRefresh(SignificantTermsIndex).execute().actionGet();
    }

    private <K, V> ImmutableMap<K, V> map(Object... objs) {
        final ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        checkState((objs.length % 2) == 0);
        for (int i = 0; i < objs.length; i += 2) {
            //noinspection unchecked
            builder.put((K) objs[i], (V) objs[i + 1]);
        }
        return builder.build();
    }

    @Test public void testSearch() {
        final String facetName = "myfacet";
        final String suggestionName = "mysugg";
        final String stringAggregationName = "mytermsagg1";
        final String doubleAggregationName = "mytermsagg2";
        final String longAggregationName = "mytermsagg3";
        final String subAggregationName = "mytermsagg3-sub";
        final String countAggName = "mycountagg1";
        final String avgAggName = "myavgagg1";
        final String minAggName = "myMinAgg1";
        final String maxAggName = "myMaxAgg1";
        final String sumAggName = "mySumAgg1";
        final String statsAggName = "myStatsAgg1";
        final String estatsAggName = "myEStatsAgg1";
        final String percentilesAggName = "myPercentilesAgg1";
        final String percentileRanksAggName = "myPercentileRanksAgg1";
        final String cardinalityAggName = "myCardinalityAgg1";
        final String geoBoundsAggName = "myGeoBoundsAgg1";
        final String topHitsAggName = "myTopHitsAgg1";
        final String scriptAggName = "myScriptAgg1";
        final String globalAggName = "myGlobalAgg1";
        final String filterAggName = "myFilterAgg1";
        final String filtersAggName = "myFiltersAgg1";
        final String filtersAggName2 = "myFiltersAgg2";
        final String missingAggName = "myMissingAgg";
        final String nestedAggName = "myNestedAgg1";
        final String nestedAggName2 = "myNestedAgg2";
        final String reverseNestedAggName = "myReverseNestedAgg1";
        final String childrenAggName = "myChildrenAgg1";
        final String rangeAggName = "myRangeAgg1";
        final String rangeAggNameKeyed = "myRangeAgg2";

        SearchRequestBuilder searchRequestBuilder = restClient().prepareSearch(INDEX);
        searchRequestBuilder.setQuery(QueryBuilders.termQuery("field", "value"));
        searchRequestBuilder.addFacet(FacetBuilders.termsFacet(facetName).field("field").size(10));
        searchRequestBuilder.addSuggestion(new TermSuggestionBuilder(suggestionName).text("valeu").field("field"));
        searchRequestBuilder.addAggregation(terms(stringAggregationName).field("field"));
        searchRequestBuilder.addAggregation(terms(doubleAggregationName).field("dfield"));
        searchRequestBuilder.addAggregation(terms(longAggregationName).field("ifield").subAggregation(terms(subAggregationName).field("field")));
        searchRequestBuilder.addAggregation(AggregationBuilders.count(countAggName).field("field"));
        searchRequestBuilder.addAggregation(AggregationBuilders.avg(avgAggName).field("ifield"));
        searchRequestBuilder.addAggregation(AggregationBuilders.min(minAggName).field("ifield"));
        searchRequestBuilder.addAggregation(AggregationBuilders.max(maxAggName).field("ifield"));
        searchRequestBuilder.addAggregation(AggregationBuilders.sum(sumAggName).field("ifield"));
        searchRequestBuilder.addAggregation(AggregationBuilders.stats(statsAggName).field("ifield"));
        searchRequestBuilder.addAggregation(AggregationBuilders.extendedStats(estatsAggName).field("ifield"));
        searchRequestBuilder.addAggregation(AggregationBuilders.percentiles(percentilesAggName).field("ifield"));
        searchRequestBuilder.addAggregation(AggregationBuilders.percentileRanks(percentileRanksAggName).field("ifield").percentiles(0, 4, 8));
        searchRequestBuilder.addAggregation(AggregationBuilders.cardinality(cardinalityAggName).field("field"));
        searchRequestBuilder.addAggregation(AggregationBuilders.geoBounds(geoBoundsAggName).field("location"));
        searchRequestBuilder.addAggregation(AggregationBuilders.topHits(topHitsAggName).setFetchSource(true));
        searchRequestBuilder.addAggregation(AggregationBuilders.scriptedMetric(scriptAggName).lang("groovy")
                .mapScript("_agg['touch'] = 1")
        );
        searchRequestBuilder.addAggregation(AggregationBuilders.global(globalAggName).subAggregation(terms(subAggregationName).field("field")));
        searchRequestBuilder.addAggregation(AggregationBuilders.filter(filterAggName).filter(FilterBuilders.termFilter("field", "value")));
        searchRequestBuilder.addAggregation(AggregationBuilders.filters(filtersAggName)
                .filter("yes", FilterBuilders.termFilter("field", "value"))
                .filter("no", FilterBuilders.termFilter("field", "missing"))
                .subAggregation(terms(subAggregationName).field("field"))
        );
        searchRequestBuilder.addAggregation(AggregationBuilders.filters(filtersAggName2)
                .filter(FilterBuilders.termFilter("field", "value"))
                .filter(FilterBuilders.termFilter("field", "missing"))
                .subAggregation(terms(subAggregationName).field("field"))
        );
        searchRequestBuilder.addAggregation(AggregationBuilders.missing(missingAggName).field("absent"));
        searchRequestBuilder.addAggregation(AggregationBuilders.nested(nestedAggName).path("things").subAggregation(terms(subAggregationName).field("things.field")));
        searchRequestBuilder.addAggregation(AggregationBuilders
            .nested(nestedAggName2).path("things")
            .subAggregation(
                terms(subAggregationName).field("things.field").subAggregation(
                    reverseNested(reverseNestedAggName).subAggregation(terms(subAggregationName).field("field")))));
        searchRequestBuilder.addAggregation(AggregationBuilders.children(childrenAggName).childType(TYPE2).subAggregation(terms(subAggregationName).field("(id)")));
        searchRequestBuilder.addAggregation(AggregationBuilders.range(rangeAggName).field("dfield").addUnboundedTo(2).addRange(2, 3).addUnboundedFrom(3));
        // FIXME upstream? There's no support in Java for setting the "keyed" param on the request.
        // FIXME upstream? Also, it's not clear what should happen if some ranges specify a key and others do not. I assume the server will barf on you.
        searchRequestBuilder.addAggregation(AggregationBuilders.range(rangeAggNameKeyed).field("dfield").addUnboundedTo("small", 2).addRange("medium", 2, 3).addUnboundedFrom("large", 3));

        ListenableActionFuture<SearchResponse> execute2 = searchRequestBuilder.execute();
        SearchResponse searchResponse = execute2.actionGet();

        assertTrue(searchResponse.getTook().millis() > 0);
        assertEquals(searchResponse.getTotalShards(), 5);
        assertEquals(searchResponse.getSuccessfulShards(), 5);
        assertEquals(searchResponse.getFailedShards(), 0);
        assertEquals(searchResponse.getShardFailures().length, 0);
        assertNull(searchResponse.getScrollId());
        final TermsFacet myfacet = searchResponse.getFacets().facet("myfacet");
        assertEquals(myfacet.getName(), facetName);
        assertEquals(myfacet.getType(), "terms");
        assertEquals(myfacet.getTotalCount(), 1);
        assertEquals(myfacet.getMissingCount(), 0);
        assertEquals(myfacet.getOtherCount(), 0);

        assertEquals(myfacet.getEntries().size(), 1);
        for (TermsFacet.Entry entry : myfacet.getEntries()) {
            assertEquals(entry.getTerm().string(), "value");
            assertEquals(entry.getCount(), 1);
        }
        {
            final Terms agg = typed(searchResponse.getAggregations()).getTerms(stringAggregationName);
            assertEquals(agg.getBuckets().size(), 1);
            for (Terms.Bucket bucket : agg.getBuckets()) {
                assertEquals(bucket.getKey(), "value");
                assertEquals(bucket.getDocCount(), 1);
            }
        }
        {
            final Terms agg = typed(searchResponse.getAggregations()).getTerms(doubleAggregationName);
            assertEquals(agg.getBuckets().size(), 1);
            for (Terms.Bucket bucket : agg.getBuckets()) {
                assertEquals(bucket.getKeyAsNumber(), (Number) 2.3);
                assertEquals(bucket.getDocCount(), 1);
            }
        }
        {
            final Terms agg = typed(searchResponse.getAggregations()).getTerms(longAggregationName);
            assertEquals(agg.getBuckets().size(), 1);
            for (Terms.Bucket bucket : agg.getBuckets()) {
                assertEquals(bucket.getKeyAsNumber(), (Number) 4l);
                assertEquals(bucket.getDocCount(), 1);
                // then test the subaggregation
                final Terms aubAgg = typed(bucket.getAggregations()).getTerms(subAggregationName);
                assertEquals(aubAgg.getBuckets().size(), 1);
                for (Terms.Bucket subBucket : aubAgg.getBuckets()) {
                    assertEquals(subBucket.getKey(), "value");
                    assertEquals(subBucket.getDocCount(), 1);
                }
            }
        }
        {
            final ValueCount agg = typed(searchResponse.getAggregations()).getValueCount(countAggName);
            assertEquals(agg.getValue(), 1);
        }
        {
            final Avg agg = typed(searchResponse.getAggregations()).getAvg(avgAggName);
            assertEquals(agg.getValue(), 4.0);
        }
        {
            final Min agg = typed(searchResponse.getAggregations()).getMin(minAggName);
            assertEquals(agg.getValue(), 4.0);
        }
        {
            final Max agg = typed(searchResponse.getAggregations()).getMax(maxAggName);
            assertEquals(agg.getValue(), 4.0);
        }
        {
            final Sum agg = typed(searchResponse.getAggregations()).getSum(sumAggName);
            assertEquals(agg.getValue(), 4.0);
        }
        {
            final Stats agg = typed(searchResponse.getAggregations()).getStats(statsAggName);
            assertEquals(agg.getCount(), 1l);
            assertEquals(agg.getAvg(), 4.0);
            assertEquals(agg.getMax(), 4.0);
            assertEquals(agg.getMin(), 4.0);
            assertEquals(agg.getSum(), 4.0);
        }
        {
            final ExtendedStats agg = typed(searchResponse.getAggregations()).getExtendedStats(estatsAggName);
            assertEquals(agg.getCount(), 1l);
            assertEquals(agg.getAvg(), 4.0);
            assertEquals(agg.getMax(), 4.0);
            assertEquals(agg.getMin(), 4.0);
            assertEquals(agg.getSum(), 4.0);
            assertEquals(agg.getStdDeviation(), 0.0);
            assertEquals(agg.getStdDeviationBound(ExtendedStats.Bounds.LOWER), Double.NaN);
            assertEquals(agg.getStdDeviationBound(ExtendedStats.Bounds.UPPER), Double.NaN);
            assertEquals(agg.getSumOfSquares(), 16.0);
            assertEquals(agg.getVariance(), 0.0);
        }

        {
            final Percentiles agg = typed(searchResponse.getAggregations()).getPercentiles(percentilesAggName);
            assertEquals(agg.percentile(100), 4.0);
        }

        {
            final PercentileRanks agg = typed(searchResponse.getAggregations()).getPercentileRanks(percentileRanksAggName);
            assertEquals(agg.percent(0.0), 0.0);
            assertEquals(agg.percent(2.0), 50.0);
            assertEquals(agg.percent(4.0), 100.0);
            assertEquals(agg.percent(8.0), 100.0);
            assertEquals(agg.percent(10.0), 100.0);
        }
        {
            final Cardinality agg = typed(searchResponse.getAggregations()).getCardinality(cardinalityAggName);
            assertEquals(agg.getValue(), 1);
        }
        {
            final GeoBounds agg = typed(searchResponse.getAggregations()).getGeoBounds(geoBoundsAggName);
//            "location", ImmutableMap.of("lat", 41.12, "lon", -71.34)
            assertEquals(agg.topLeft().lat(), 41.12);
            assertEquals(agg.topLeft().lon(), -71.34);
            assertEquals(agg.bottomRight().lat(), 41.12);
            assertEquals(agg.bottomRight().lon(), -71.34);
        }

        {
            final TopHits agg = typed(searchResponse.getAggregations()).getTopHits(topHitsAggName);
            assertEquals(agg.getHits().getTotalHits(), 1);
            final SearchHit hit = agg.getHits().getAt(0);
            assertEquals(hit.index(), INDEX);
            assertEquals(hit.getType(), TYPE);
            assertEquals(hit.getId(), ID1);
            assertEquals(hit.getSource().get("field"), "value");
        }

        {
            final ScriptedMetric agg = typed(searchResponse.getAggregations()).getScriptedMetric(scriptAggName);
            int sum = 0;
            for (Map<String, Integer> bucket : (List<Map<String, Integer>>) agg.aggregation()) {
                if (!bucket.isEmpty()) {
                    sum += bucket.get("touch");
                }
            }
            assertEquals(sum, 1);
        }

        {
            final Global agg = typed(searchResponse.getAggregations()).getGlobal(globalAggName);
            assertEquals(agg.getDocCount(), 2);
            // then test the subaggregation
            final Terms subAgg = typed(agg.getAggregations()).getTerms(subAggregationName);
            assertEquals(subAgg.getBuckets().size(), 1);
            for (Terms.Bucket subBucket : subAgg.getBuckets()) {
                assertEquals(subBucket.getKey(), "value");
                assertEquals(subBucket.getDocCount(), 1);
            }
        }

        {
            final Filter agg = typed(searchResponse.getAggregations()).getFilter(filterAggName);
            assertEquals(agg.getDocCount(), 1);
            assertEquals(agg.getAggregations().asList().size(), 0);
        }

        {
            final Filters agg = typed(searchResponse.getAggregations()).getFilters(filtersAggName);
            {
                assertEquals(agg.getBucketByKey("yes").getDocCount(), 1);
                final Aggregations subAggs = agg.getBucketByKey("yes").getAggregations();
                assertEquals(subAggs.asList().size(), 1);
                final Terms subAgg = typed(subAggs).getTerms(subAggregationName);
                assertEquals(subAgg.getBuckets().size(), 1);
                for (Terms.Bucket subBucket : subAgg.getBuckets()) {
                    assertEquals(subBucket.getKey(), "value");
                    assertEquals(subBucket.getDocCount(), 1);
                }
            }
            {
                assertEquals(agg.getBucketByKey("no").getDocCount(), 0);
                final Aggregations subAggs = agg.getBucketByKey("no").getAggregations();
                assertEquals(typed(subAggs).getTerms(subAggregationName).getBuckets().size(), 0);
            }
        }

        {
            final Filters agg = typed(searchResponse.getAggregations()).getFilters(filtersAggName2);
            assertEquals(agg.getBuckets().size(), 2);
            long totalDocs = 0;
            int totalAggs = 0;
            for (Filters.Bucket bucket : agg.getBuckets()) {
                totalDocs += bucket.getDocCount();
                final Terms.Bucket subAgg = typed(bucket.getAggregations()).getTerms(subAggregationName).getBucketByKey("value");
                if (subAgg != null) {
                    assertEquals(subAgg.getDocCount(), 1);
                    totalAggs++;
                }
            }
            assertEquals(totalAggs, 1);
            assertEquals(totalDocs, 1);
        }

        {
            final Missing agg = typed(searchResponse.getAggregations()).getMissing(missingAggName);
            assertEquals(agg.getDocCount(), 1);
        }

        {
            final Nested agg = typed(searchResponse.getAggregations()).getNested(nestedAggName);
            assertEquals(agg.getDocCount(), 1);
            final Terms.Bucket subAgg = typed(agg.getAggregations()).getTerms(subAggregationName).getBucketByKey("nested");
            assertEquals(subAgg.getDocCount(), 1);
        }

        {
            final Nested agg = typed(searchResponse.getAggregations()).getNested(nestedAggName2);
            assertEquals(agg.getDocCount(), 1);

            final TypedAggregations topLevelAggs = typed(agg.getAggregations());
            final Terms.Bucket subAgg = topLevelAggs.getTerms(subAggregationName).getBucketByKey("nested");
            assertEquals(subAgg.getDocCount(), 1);

            final TypedAggregations nestedAggs = typed(topLevelAggs.getTerms(subAggregationName).getBucketByKey("nested").getAggregations());
            final ReverseNested reverseNested = nestedAggs.getReverseNested(reverseNestedAggName);
            assertEquals(reverseNested.getDocCount(), 1);

            final TypedAggregations reverseNestedAggs = typed(nestedAggs.getReverseNested(reverseNestedAggName).getAggregations());
            final Terms.Bucket reverseNestedBucket = reverseNestedAggs.getTerms(subAggregationName).getBucketByKey("value");
            assertEquals(reverseNestedBucket.getDocCount(), 1);
        }

        {
            final Children agg = typed(searchResponse.getAggregations()).getChildren(childrenAggName);
            assertEquals(agg.getDocCount(), 1);
            assertEquals(typed(agg.getAggregations()).getTerms(subAggregationName).getBuckets().size(), 1);
            final Terms.Bucket bucket = typed(agg.getAggregations()).getTerms(subAggregationName).getBucketByKey(ID2);
            assertEquals(bucket.getDocCount(), 1);
        }

        {
            final Range agg = typed(searchResponse.getAggregations()).getRange(rangeAggName);
            assertEquals(agg.getBuckets().size(), 3);
            for (Range.Bucket bucket : agg.getBuckets()) {
                if (bucket.getDocCount() == 1) {
                    assertEquals(bucket.getFrom().doubleValue(), 2.0);
                    assertEquals(bucket.getTo().doubleValue(), 3.0);
                } else {
                    assertEquals(bucket.getDocCount(), 0);
                }
            }
        }

        {
            final Range agg = typed(searchResponse.getAggregations()).getRange(rangeAggNameKeyed);
            assertEquals(agg.getBuckets().size(), 3);
            assertEquals(agg.getBucketByKey("small").getFrom().doubleValue(), Double.NEGATIVE_INFINITY);
            assertEquals(agg.getBucketByKey("small").getTo().doubleValue(), 2.0);
            assertEquals(agg.getBucketByKey("small").getDocCount(), 0);

            assertEquals(agg.getBucketByKey("medium").getFrom().doubleValue(), 2.0);
            assertEquals(agg.getBucketByKey("medium").getTo().doubleValue(), 3.0);
            assertEquals(agg.getBucketByKey("medium").getDocCount(), 1);

            assertEquals(agg.getBucketByKey("large").getFrom().doubleValue(), 3.0);
            assertEquals(agg.getBucketByKey("large").getTo().doubleValue(), Double.POSITIVE_INFINITY);
            assertEquals(agg.getBucketByKey("large").getDocCount(), 0);
        }


        final Suggest.Suggestion<Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option>> suggestion = searchResponse.getSuggest().getSuggestion(suggestionName);
        final List<Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option>> entries = suggestion.getEntries();
        assertEquals(entries.size(), 1);
        for (Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option> entry : entries) {
            assertEquals(entry.getText().string(), "valeu");
            assertEquals(entry.getOffset(), 0);
            assertEquals(entry.getLength(), 5);
            assertEquals(entry.getOptions().size(), 1);
            for (Suggest.Suggestion.Entry.Option option : entry.getOptions()) {
                assertEquals(option.getText().string(), "value");
                assertNull(option.getHighlighted().string());
                assertTrue(option.getScore() > 0.0);
            }
        }

        assertTrue(searchResponse.getHits().getMaxScore() > 0.0);
        assertEquals(searchResponse.getHits().getTotalHits(), 1);
        assertEquals(searchResponse.getHits().getHits().length, 1);
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            assertEquals(hit.index(), INDEX);
            /* ignoring, unserialized...*/
            System.out.println("type: " + Objects.toString(hit.getType()));
            assertEquals(hit.getType(), TYPE);
            assertEquals(hit.getId(), ID1);
            /* ignoring, unserialized...*/
            System.out.println("source: " + Objects.toString(hit.getSourceAsString()));
            assertEquals(hit.getSource().get("field"), "value");
            assertNull(hit.explanation());
            assertTrue(hit.getFields().isEmpty());
            assertTrue(hit.getHighlightFields().isEmpty());
            /* ignoring, unserialized...*/
            System.out.println("sortValues#: " + Objects.toString(hit.getSortValues().length));
            assertEquals(hit.getSortValues().length, 0);
            assertEquals(hit.getMatchedQueries().length, 0);
        }
    }

    @Test public void testSignificantTerms() {
        /*
        This gets a bit confusing, so I'll document how everything maps to everything else.
        So the math goes like this:
        A term is significant if this number:
        termFrequencyForGroup/allTermsFrequencyForGroup
        is somehow bigger than this number:
        termFrequencyOverall/allTermsFrequencyOverall

        Here's how those map to ES api as well as SignificantTerms fields:
        termFrequencyForGroup = bucket.doc_count = subsetDf
        allTermsFrequencyForGroup = aggregation.doc_count = subsetSize
        termFrequencyOverall = bucket.bg_count = supersetDf
        allTermsFrequencyOverall = (unserialized in API) = supersetSize

        I'd like to send a PR in the future to serialize the supersetSize.
        By default this number is actually just the number of documents in the
        index, but you can narrow the scope using "background_filter", in which
        case the supersetSize is the number of docs matching the filter.

        I'll explain the Metro Police example:
        termFrequencyForGroup is 20 because Metro saw 20 robberies.
        allTermsForGroup is 21 because Metro saw 21 crimes overall.
        termFrequencyOverall is 22, because there were 22 robberies over Metro,Transport, and APD.
        allTermsFrequencyOverall is 33 because there are a total of 33 documents in the index
        (they all happen to be crimes).
         */


        final String termsAggName = "forces";
        final String significantTermsAggName = "significantCrimeTypes";
        SearchRequestBuilder searchRequestBuilder = restClient().prepareSearch(SignificantTermsIndex);
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        searchRequestBuilder.addAggregation(
            terms(termsAggName).field("force").subAggregation(
                significantTerms(significantTermsAggName).field("crime_type")));
        final SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        {
            final Terms agg = typed(searchResponse.getAggregations()).getTerms(termsAggName);
            assertEquals(agg.getBuckets().size(), 3);

            {
                final Terms.Bucket bucket = agg.getBucketByKey(metroPolice);
                assertEquals(bucket.getDocCount(), 21);
                final SignificantTerms significantTerms = typed(bucket.getAggregations()).getSignificantTerms(significantTermsAggName);
                final SignificantTerms.Bucket sigTermsBucket = significantTerms.getBucketByKey(robbery);
                assertNotNull(sigTermsBucket);
                assertEquals(sigTermsBucket.getDocCount(), 20);
                assertEquals(sigTermsBucket.getSubsetDf(), 20);
                assertEquals(sigTermsBucket.getSubsetSize(), 21);
                assertEquals(sigTermsBucket.getSupersetDf(), 22);
                try {
                    sigTermsBucket.getSupersetSize(); // would be 33
                    fail();
                } catch (UnsupportedOperationException e) {
                    assertEquals(e.getMessage(), "unserialized information");
                }
                Assert.assertTrue(Double.toString(sigTermsBucket.getSignificanceScore()),
                    !Double.isInfinite(sigTermsBucket.getSignificanceScore()) &&
                        !Double.isNaN(sigTermsBucket.getSignificanceScore()) &&
                        sigTermsBucket.getSignificanceScore() > 0

                );
            }

            {
                final Terms.Bucket bucket = agg.getBucketByKey(transportPolice);
                assertEquals(bucket.getDocCount(), 11);
                final SignificantTerms significantTerms = typed(bucket.getAggregations()).getSignificantTerms(significantTermsAggName);
                final SignificantTerms.Bucket sigTermsBucket = significantTerms.getBucketByKey(bikeTheft);
                assertNotNull(sigTermsBucket);
                assertEquals(sigTermsBucket.getDocCount(), 10);
                assertEquals(sigTermsBucket.getSubsetDf(), 10);
                assertEquals(sigTermsBucket.getSubsetSize(), 11);
                assertEquals(sigTermsBucket.getSupersetDf(), 12);
                try {
                    sigTermsBucket.getSupersetSize(); // would be 33
                    fail();
                } catch (UnsupportedOperationException e) {
                    assertEquals(e.getMessage(), "unserialized information");
                }
                Assert.assertTrue(Double.toString(sigTermsBucket.getSignificanceScore()),
                    !Double.isInfinite(sigTermsBucket.getSignificanceScore()) &&
                        !Double.isNaN(sigTermsBucket.getSignificanceScore()) &&
                        sigTermsBucket.getSignificanceScore() > 0
                );
            }

            {
                final Terms.Bucket bucket = agg.getBucketByKey("APD");
                assertEquals(bucket.getDocCount(), 2);
                final SignificantTerms significantTerms = typed(bucket.getAggregations()).getSignificantTerms(significantTermsAggName);
                assertEquals(significantTerms.getBuckets().size(), 0); // no significant terms for these folks
            }
        }
    }
}
