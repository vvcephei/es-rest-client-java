package com.bazaarvoice.elasticsearch.client;

import com.bazaarvoice.elasticsearch.client.core.TypedAggregations;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class SearchTest extends JerseyRestClientTest {

    public static final String INDEX = "search-test-idx";
    public static final String TYPE = "search-test-type";
    public static final String ID = "search-test-id-1";

    private static boolean debug = false;

    @BeforeClass
    public void setupSearchTest() {
        restClient().prepareIndex(INDEX, TYPE, ID).setSource("field", "value", "dfield", 2.3, "ifield", 4).setRefresh(true).execute().actionGet();
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


        SearchRequestBuilder searchRequestBuilder = restClient().prepareSearch(INDEX);
        searchRequestBuilder.setQuery(QueryBuilders.termQuery("field", "value"));
        searchRequestBuilder.addFacet(FacetBuilders.termsFacet(facetName).field("field").size(10));
        searchRequestBuilder.addSuggestion(new TermSuggestionBuilder(suggestionName).text("valeu").field("field"));
        searchRequestBuilder.addAggregation(terms(stringAggregationName).field("field"));
        searchRequestBuilder.addAggregation(terms(doubleAggregationName).field("dfield"));
        searchRequestBuilder.addAggregation(terms(longAggregationName).field("ifield").subAggregation(terms(subAggregationName).field("field")));
        searchRequestBuilder.addAggregation(AggregationBuilders.count(countAggName).field("field"));
        searchRequestBuilder.addAggregation(AggregationBuilders.avg(avgAggName).field("ifield"));
        ListenableActionFuture<SearchResponse> execute2 = searchRequestBuilder.execute();
        SearchResponse searchResponse = execute2.actionGet();

        if (debug) System.out.println("took: " + Objects.toString(searchResponse.getTook()));
        if (debug) System.out.println("tookMillis: " + Objects.toString(searchResponse.getTookInMillis()));
        assertTrue(searchResponse.getTook().millis() > 0);
        if (debug) System.out.println("totalShards: " + Objects.toString(searchResponse.getTotalShards()));
        if (debug) System.out.println("successfulShards: " + Objects.toString(searchResponse.getSuccessfulShards()));
        if (debug) System.out.println("failedShards: " + Objects.toString(searchResponse.getFailedShards()));
        if (debug) System.out.println("shardFailures#: " + Objects.toString(searchResponse.getShardFailures().length));
        assertEquals(searchResponse.getTotalShards(), 5);
        assertEquals(searchResponse.getSuccessfulShards(), 5);
        assertEquals(searchResponse.getFailedShards(), 0);
        assertEquals(searchResponse.getShardFailures().length, 0);
        if (debug) System.out.println("scrollId: " + Objects.toString(searchResponse.getScrollId()));
        assertNull(searchResponse.getScrollId());
        if (debug) System.out.println("facets: " + Objects.toString(searchResponse.getFacets()));
        final TermsFacet myfacet = searchResponse.getFacets().facet("myfacet");
        if (debug) System.out.println("facet: name: " + Objects.toString(myfacet.getName()));
        assertEquals(myfacet.getName(), facetName);
        if (debug) System.out.println("facet: type: " + Objects.toString(myfacet.getType()));
        assertEquals(myfacet.getType(), "terms");
        if (debug) System.out.println("facet: total: " + Objects.toString(myfacet.getTotalCount()));
        assertEquals(myfacet.getTotalCount(), 1);
        if (debug) System.out.println("facet: missing: " + Objects.toString(myfacet.getMissingCount()));
        assertEquals(myfacet.getMissingCount(), 0);
        if (debug) System.out.println("facet: other: " + Objects.toString(myfacet.getOtherCount()));
        assertEquals(myfacet.getOtherCount(), 0);

        assertEquals(myfacet.getEntries().size(), 1);
        for (TermsFacet.Entry entry : myfacet.getEntries()) {
            if (debug) System.out.println("facet: entry: term: " + Objects.toString(entry.getTerm()));
            assertEquals(entry.getTerm().string(), "value");
            if (debug) System.out.println("facet: entry: count: " + Objects.toString(entry.getCount()));
            assertEquals(entry.getCount(), 1);
        }
        if (debug) System.out.println("aggs: " + Objects.toString(searchResponse.getAggregations()));
        {
            final StringTerms aggregation = searchResponse.getAggregations().get(stringAggregationName);


            final Terms stringTerms = TypedAggregations.wrap(searchResponse.getAggregations()).getTerms(stringAggregationName);
            assertEquals(stringTerms.getBuckets().size(), 1);
            for (Terms.Bucket bucket : stringTerms.getBuckets()) {
                assertEquals(bucket.getKey(), "value");
                assertEquals(bucket.getDocCount(), 1);
            }
        }
        {
            final Terms doubleTerms = TypedAggregations.wrap(searchResponse.getAggregations()).getTerms(doubleAggregationName);
            assertEquals(doubleTerms.getBuckets().size(), 1);
            for (Terms.Bucket bucket : doubleTerms.getBuckets()) {
                assertEquals(bucket.getKeyAsNumber(), (Number) 2.3);
                assertEquals(bucket.getDocCount(), 1);
            }
        }
        {
            final Terms longTerms = TypedAggregations.wrap(searchResponse.getAggregations()).getTerms(longAggregationName);
            assertEquals(longTerms.getBuckets().size(), 1);
            for (Terms.Bucket bucket : longTerms.getBuckets()) {
                assertEquals(bucket.getKeyAsNumber(), (Number) 4l);
                assertEquals(bucket.getDocCount(), 1);
                // then test the subaggregation
                final Terms stringTerms = TypedAggregations.wrap(bucket.getAggregations()).getTerms(subAggregationName);
                assertEquals(stringTerms.getBuckets().size(), 1);
                for (Terms.Bucket subBucket : stringTerms.getBuckets()) {
                    assertEquals(subBucket.getKey(), "value");
                    assertEquals(subBucket.getDocCount(), 1);
                }
            }
        }
        {
            final ValueCount valueCount = TypedAggregations.wrap(searchResponse.getAggregations()).getValueCount(countAggName);
            assertEquals(valueCount.getValue(), 1);
        }
        {
            final Avg valueCount = TypedAggregations.wrap(searchResponse.getAggregations()).getAvg(avgAggName);
            assertEquals(valueCount.getValue(), 4.0);
        }


        if (debug) System.out.println("suggest: " + Objects.toString(searchResponse.getSuggest()));
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

        if (debug) System.out.println("maxScore" + Objects.toString(searchResponse.getHits().getMaxScore()));
        assertTrue(searchResponse.getHits().getMaxScore() > 0.0);
        if (debug) System.out.println("totalHits: " + Objects.toString(searchResponse.getHits().getTotalHits()));
        assertEquals(searchResponse.getHits().getTotalHits(), 1);
        if (debug) System.out.println("hits: " + Objects.toString(searchResponse.getHits().getHits()));
        assertEquals(searchResponse.getHits().getHits().length, 1);
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            if (debug) System.out.println("index: " + Objects.toString(hit.getIndex()));
            assertEquals(hit.index(), INDEX);
            if (debug) System.out.println("shard: " + Objects.toString(hit.getShard()));
            // ignoring, unserialized...
            if (debug) System.out.println("type: " + Objects.toString(hit.getType()));
            assertEquals(hit.getType(), TYPE);
            if (debug) System.out.println("id: " + Objects.toString(hit.getId()));
            assertEquals(hit.getId(), ID);
            if (debug) System.out.println("version: " + Objects.toString(hit.getVersion()));
            // ignoring, unserialized...
            if (debug) System.out.println("source: " + Objects.toString(hit.getSourceAsString()));
            assertEquals(hit.getSource().get("field"), "value");
            if (debug) System.out.println("explanation: " + Objects.toString(hit.getExplanation()));
            assertNull(hit.explanation());
            if (debug) System.out.println("fields: " + Objects.toString(hit.getFields()));
            assertTrue(hit.getFields().isEmpty());
            if (debug) System.out.println("highlightFields: " + Objects.toString(hit.getHighlightFields()));
            assertTrue(hit.getHighlightFields().isEmpty());
            if (debug) System.out.println("score: " + Objects.toString(hit.getScore()));
            // ignoring, unserialized...
            if (debug) System.out.println("sortValues#: " + Objects.toString(hit.getSortValues().length));
            assertEquals(hit.getSortValues().length, 0);
            if (debug) System.out.println("matchedQueries#: " + Objects.toString(hit.getMatchedQueries().length));
            assertEquals(hit.getMatchedQueries().length, 0);
        }
    }
}
