package com.bazaarvoice.elasticsearch.client;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class GetTest extends JerseyHttpClientTest {

    public static final String ID = "get-test-id-1";

    @Test public void testGet() {
        final IndexRequestBuilder indexRequestBuilder = restClient().prepareIndex("testidx", "testtype", ID).setSource("field", "value").setRefresh(true);
        indexRequestBuilder.execute().actionGet();


        final GetRequestBuilder getRequestBuilder = restClient().prepareGet("testidx", "testtype", ID);
        final ListenableActionFuture<GetResponse> execute1 = getRequestBuilder.execute();
        final GetResponse get = execute1.actionGet();
        assertEquals(get.getIndex(), "testidx");
        assertEquals(get.getType(), "testtype");
        assertEquals(get.getId(), ID);
        assertEquals(get.getVersion(), 1);
        assertTrue(get.getFields().isEmpty());
        assertEquals(get.getSource().get("field"), "value");
    }
}
