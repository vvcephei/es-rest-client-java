package org.elasticsearch;

import org.elasticsearch.index.Index;
import org.elasticsearch.indices.RestIndexMissingException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ElasticSearchExceptionHelperTest {

    @DataProvider (parallel = true)
    public Object[][] getProperExceptionTests_IndexMissing() {
        return new Object[][] {
                { newMap("error", "IndexMissingException[[INDEX_NAME] missing]"),  new RestIndexMissingException(new Index("INDEX_NAME"), newMap("error", "IndexMissingException[[INDEX_NAME] missing]"))},
                { newMap("error", "IndexMissingException[[ INDEX_NAME] missing]"), new RestIndexMissingException(new Index(" INDEX_NAME"), newMap("error", "IndexMissingException[[ INDEX_NAME] missing]"))},
                { newMap("error", "IndexMissingException[[INDEX_NAME ] missing]"), new RestIndexMissingException(new Index("INDEX_NAME "), newMap("error", "IndexMissingException[[INDEX_NAME ] missing]"))}
        };
    }

    @Test(dataProvider = "getProperExceptionTests_IndexMissing")
    public void testGetProperException_IndexMissing(Map<String, Object> resultsMap, RestIndexMissingException expectedException) {
        RuntimeException ex = ElasticSearchExceptionHelper.getProperException(resultsMap);
        assertEquals(ex.getClass(), RestIndexMissingException.class);
        RestIndexMissingException actual = (RestIndexMissingException) ex;
        assertEquals(actual.index().name(), expectedException.index().name());
        assertEquals(actual.getRepsonseMap(), expectedException.getRepsonseMap());
    }

    @DataProvider (parallel = true)
    public Object[][] getProperExceptionTests_Generic() {
        return new Object[][] {
                { new HashMap<String, Object>(), new RuntimeException("")},
                { newMap("error", "SomeStrangeError"), new RuntimeException("SomeStrangeError") },
                { newMap("error", new NullPointerException("dumdum")), new RuntimeException("dumdum") },
                { newMap("error", "IndexMissingException [[INDEX_NAME] missing]"),   new RuntimeException("IndexMissingException [[INDEX_NAME] missing")} // space after exception name
        };
    }

    @Test(dataProvider = "getProperExceptionTests_Generic")
    public void testGetProperException_Generic(Map<String, Object> resultsMap, RuntimeException expectedException) {
        RuntimeException ex = ElasticSearchExceptionHelper.getProperException(resultsMap);
        assertEquals(ex.getClass(), RuntimeException.class);
        assertTrue(ex.getMessage().contains(expectedException.getMessage()));
    }

    private Map<String, Object> newMap(String key, Object value) {
        Map<String, Object> rv = new HashMap<String, Object>();
        rv.put(key, value);
        return rv;
    }
}
