package com.bazaarvoice.elasticsearch.client;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class DeleteTest extends JerseyHttpClientTest {

    public static final String ID = "delete-test-id-1";

    @Test public void testDelete() {
        final IndexRequestBuilder indexRequestBuilder = restClient().prepareIndex("testidx", "testtype", ID).setSource("field", "value").setRefresh(true);
        indexRequestBuilder.execute().actionGet();


        final DeleteRequestBuilder deleteRequestBuilder = restClient().prepareDelete("testidx", "testtype", ID);
        final DeleteResponse deleteResponse = deleteRequestBuilder.execute().actionGet();
        assertEquals(deleteResponse.getIndex(), "testidx");
        assertEquals(deleteResponse.getType(), "testtype");
        assertEquals(deleteResponse.getId(), ID);
        assertEquals(deleteResponse.getVersion(), 2);
        assertTrue(deleteResponse.isFound());
    }
}
