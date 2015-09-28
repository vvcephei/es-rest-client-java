package com.bazaarvoice.elasticsearch.client;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class GetTest extends JerseyRestClientTest {

    private final String index = "get-test-idx";
    private final String type = "get-test-type";

    @Test public void testGet() {
        final String id = "get-test-id-1";
        final IndexRequestBuilder indexRequestBuilder = restClient().prepareIndex(index, type, id).setSource("field", "value").setRefresh(true);
        indexRequestBuilder.execute().actionGet();


        final GetRequestBuilder getRequestBuilder = restClient().prepareGet(index, type, id);
        final ListenableActionFuture<GetResponse> execute1 = getRequestBuilder.execute();
        final GetResponse get = execute1.actionGet();
        assertEquals(get.getIndex(), index);
        assertEquals(get.getType(), type);
        assertEquals(get.getId(), id);
        assertEquals(get.getVersion(), 1);
        assertTrue(get.getFields().isEmpty());
        assertEquals(get.getSource().get("field"), "value");
    }

    @Test public void testGetSlash() {
        final String id = "get-test/id-2";
        final IndexRequestBuilder indexRequestBuilder = restClient().prepareIndex(index, type, id).setSource("field", "value").setRefresh(true);
        indexRequestBuilder.execute().actionGet();


        final GetRequestBuilder getRequestBuilder = restClient().prepareGet(index, type, id);
        final ListenableActionFuture<GetResponse> execute1 = getRequestBuilder.execute();
        final GetResponse get = execute1.actionGet();
        assertEquals(get.getIndex(), index);
        assertEquals(get.getType(), type);
        assertEquals(get.getId(), id);
        assertEquals(get.getVersion(), 1);
        assertTrue(get.getFields().isEmpty());
        assertEquals(get.getSource().get("field"), "value");
    }

    @Test
    public void testGet_IndexMissingException() {
        final String id = "get-test/id-2";
        final IndexRequestBuilder indexRequestBuilder = restClient().prepareIndex(index, type, id).setSource("field", "value").setRefresh(true);
        indexRequestBuilder.execute().actionGet();
        final GetRequestBuilder getRequestBuilder = restClient().prepareGet(index + "nosuch", type, id);
        final ListenableActionFuture<GetResponse> execute1 = getRequestBuilder.execute();
        try {
            execute1.actionGet();
            fail("Expected an IndexMissingException");
        } catch (IndexMissingException e) {
            assertEquals(e.index().getName(), index+"nosuch");
        }
    }
}
