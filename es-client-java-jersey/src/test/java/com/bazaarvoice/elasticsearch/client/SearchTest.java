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
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class SearchTest extends JerseyRestClientTest {

    public static final String INDEX = "search-test-idx";
    public static final String TYPE = "search-test-type";
    public static final String ID1 = "search-test-id-1";
    public static final String ID2 = "search-test-id-2";

    @BeforeClass
    public void setupSearchTest() throws IOException {
        nodeClient().admin().indices().prepareCreate(INDEX).addMapping(TYPE,
            ImmutableMap.<String, Object>of(
                "properties", ImmutableMap.of(
                    "field", ImmutableMap.of("type", "string"),
                    "dfield", ImmutableMap.of("type", "double"),
                    "ifield", ImmutableMap.of("type", "long"),
                    "location", ImmutableMap.of("type", "geo_point"),
                    "things", ImmutableMap.of("type", "nested", "properties",ImmutableMap.of("field",ImmutableMap.of("type", "string")))
                ))
        ).execute().actionGet();

        restClient().prepareIndex(INDEX, TYPE, ID1).setSource(
            "field", "value",
            "dfield", 2.3,
            "ifield", 4,
            "location", "41.12,-71.34",
            "things", ImmutableList.of(ImmutableMap.of("field","nested"))
        ).setRefresh(true).execute().actionGet();
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
            final Terms agg = TypedAggregations.wrap(searchResponse.getAggregations()).getTerms(stringAggregationName);
            assertEquals(agg.getBuckets().size(), 1);
            for (Terms.Bucket bucket : agg.getBuckets()) {
                assertEquals(bucket.getKey(), "value");
                assertEquals(bucket.getDocCount(), 1);
            }
        }
        {
            final Terms agg = TypedAggregations.wrap(searchResponse.getAggregations()).getTerms(doubleAggregationName);
            assertEquals(agg.getBuckets().size(), 1);
            for (Terms.Bucket bucket : agg.getBuckets()) {
                assertEquals(bucket.getKeyAsNumber(), (Number) 2.3);
                assertEquals(bucket.getDocCount(), 1);
            }
        }
        {
            final Terms agg = TypedAggregations.wrap(searchResponse.getAggregations()).getTerms(longAggregationName);
            assertEquals(agg.getBuckets().size(), 1);
            for (Terms.Bucket bucket : agg.getBuckets()) {
                assertEquals(bucket.getKeyAsNumber(), (Number) 4l);
                assertEquals(bucket.getDocCount(), 1);
                // then test the subaggregation
                final Terms aubAgg = TypedAggregations.wrap(bucket.getAggregations()).getTerms(subAggregationName);
                assertEquals(aubAgg.getBuckets().size(), 1);
                for (Terms.Bucket subBucket : aubAgg.getBuckets()) {
                    assertEquals(subBucket.getKey(), "value");
                    assertEquals(subBucket.getDocCount(), 1);
                }
            }
        }
        {
            final ValueCount agg = TypedAggregations.wrap(searchResponse.getAggregations()).getValueCount(countAggName);
            assertEquals(agg.getValue(), 1);
        }
        {
            final Avg agg = TypedAggregations.wrap(searchResponse.getAggregations()).getAvg(avgAggName);
            assertEquals(agg.getValue(), 4.0);
        }
        {
            final Min agg = TypedAggregations.wrap(searchResponse.getAggregations()).getMin(minAggName);
            assertEquals(agg.getValue(), 4.0);
        }
        {
            final Max agg = TypedAggregations.wrap(searchResponse.getAggregations()).getMax(maxAggName);
            assertEquals(agg.getValue(), 4.0);
        }
        {
            final Sum agg = TypedAggregations.wrap(searchResponse.getAggregations()).getSum(sumAggName);
            assertEquals(agg.getValue(), 4.0);
        }
        {
            final Stats agg = TypedAggregations.wrap(searchResponse.getAggregations()).getStats(statsAggName);
            assertEquals(agg.getCount(), 1l);
            assertEquals(agg.getAvg(), 4.0);
            assertEquals(agg.getMax(), 4.0);
            assertEquals(agg.getMin(), 4.0);
            assertEquals(agg.getSum(), 4.0);
        }
        {
            final ExtendedStats agg = TypedAggregations.wrap(searchResponse.getAggregations()).getExtendedStats(estatsAggName);
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
            final Percentiles agg = TypedAggregations.wrap(searchResponse.getAggregations()).getPercentiles(percentilesAggName);
            assertEquals(agg.percentile(100), 4.0);
        }

        {
            final PercentileRanks agg = TypedAggregations.wrap(searchResponse.getAggregations()).getPercentileRanks(percentileRanksAggName);
            assertEquals(agg.percent(0.0), 0.0);
            assertEquals(agg.percent(2.0), 50.0);
            assertEquals(agg.percent(4.0), 100.0);
            assertEquals(agg.percent(8.0), 100.0);
            assertEquals(agg.percent(10.0), 100.0);
        }
        {
            final Cardinality agg = TypedAggregations.wrap(searchResponse.getAggregations()).getCardinality(cardinalityAggName);
            assertEquals(agg.getValue(), 1);
        }
        {
            final GeoBounds agg = TypedAggregations.wrap(searchResponse.getAggregations()).getGeoBounds(geoBoundsAggName);
//            "location", ImmutableMap.of("lat", 41.12, "lon", -71.34)
            assertEquals(agg.topLeft().lat(), 41.12);
            assertEquals(agg.topLeft().lon(), -71.34);
            assertEquals(agg.bottomRight().lat(), 41.12);
            assertEquals(agg.bottomRight().lon(), -71.34);
        }

        {
            final TopHits agg = TypedAggregations.wrap(searchResponse.getAggregations()).getTopHits(topHitsAggName);
            assertEquals(agg.getHits().getTotalHits(), 1);
            final SearchHit hit = agg.getHits().getAt(0);
            assertEquals(hit.index(), INDEX);
            assertEquals(hit.getType(), TYPE);
            assertEquals(hit.getId(), ID1);
            assertEquals(hit.getSource().get("field"), "value");
        }

        {
            final ScriptedMetric agg = TypedAggregations.wrap(searchResponse.getAggregations()).getScriptedMetric(scriptAggName);
            int sum = 0;
            for (Map<String, Integer> bucket : (List<Map<String, Integer>>) agg.aggregation()) {
                if (!bucket.isEmpty()) {
                    sum += bucket.get("touch");
                }
            }
            assertEquals(sum, 1);
        }

        {
            final Global agg = TypedAggregations.wrap(searchResponse.getAggregations()).getGlobal(globalAggName);
            assertEquals(agg.getDocCount(), 1);
            // then test the subaggregation
            final Terms subAgg = TypedAggregations.wrap(agg.getAggregations()).getTerms(subAggregationName);
            assertEquals(subAgg.getBuckets().size(), 1);
            for (Terms.Bucket subBucket : subAgg.getBuckets()) {
                assertEquals(subBucket.getKey(), "value");
                assertEquals(subBucket.getDocCount(), 1);
            }
        }

        {
            final Filter agg = TypedAggregations.wrap(searchResponse.getAggregations()).getFilter(filterAggName);
            assertEquals(agg.getDocCount(), 1);
            assertEquals(agg.getAggregations().asList().size(), 0);
        }

        {
            final Filters agg = TypedAggregations.wrap(searchResponse.getAggregations()).getFilters(filtersAggName);
            {
                assertEquals(agg.getBucketByKey("yes").getDocCount(), 1);
                final Aggregations subAggs = agg.getBucketByKey("yes").getAggregations();
                assertEquals(subAggs.asList().size(), 1);
                final Terms subAgg = TypedAggregations.wrap(subAggs).getTerms(subAggregationName);
                assertEquals(subAgg.getBuckets().size(), 1);
                for (Terms.Bucket subBucket : subAgg.getBuckets()) {
                    assertEquals(subBucket.getKey(), "value");
                    assertEquals(subBucket.getDocCount(), 1);
                }
            }
            {
                assertEquals(agg.getBucketByKey("no").getDocCount(), 0);
                final Aggregations subAggs = agg.getBucketByKey("no").getAggregations();
                assertEquals(TypedAggregations.wrap(subAggs).getTerms(subAggregationName).getBuckets().size(), 0);
            }
        }

        {
            final Filters agg = TypedAggregations.wrap(searchResponse.getAggregations()).getFilters(filtersAggName2);
            assertEquals(agg.getBuckets().size(), 2);
            long totalDocs = 0;
            int totalAggs = 0;
            for (Filters.Bucket bucket : agg.getBuckets()){
                totalDocs += bucket.getDocCount();
                final Terms.Bucket subAgg = TypedAggregations.wrap(bucket.getAggregations()).getTerms(subAggregationName).getBucketByKey("value");
                if (subAgg != null) {
                    assertEquals(subAgg.getDocCount(), 1);
                    totalAggs++;
                }
            }
            assertEquals(totalAggs, 1);
            assertEquals(totalDocs, 1);
        }

        {
            final Missing agg = TypedAggregations.wrap(searchResponse.getAggregations()).getMissing(missingAggName);
            assertEquals(agg.getDocCount(), 1);
        }

        {
            final Nested agg = TypedAggregations.wrap(searchResponse.getAggregations()).getNested(nestedAggName);
            assertEquals(agg.getDocCount(), 1);
            final Terms.Bucket subAgg = TypedAggregations.wrap(agg.getAggregations()).getTerms(subAggregationName).getBucketByKey("nested");
            assertEquals(subAgg.getDocCount(), 1);
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
                // FIXME WTF? assertEquals(option.getHighlighted().string(), "null");
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
}
