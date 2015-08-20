package com.bazaarvoice.elasticsearch.client;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class IndexTest extends JerseyRestClientTest {

    public static final String ID = "index-test-id-1";

    @Test
    public void testIndex() {
        final IndexRequestBuilder indexRequestBuilder = restClient().prepareIndex("testidx", "testtype", ID).setSource("field", "value").setRefresh(true);
        final ListenableActionFuture<IndexResponse> execute = indexRequestBuilder.execute();
        final IndexResponse indexResponse = execute.actionGet();
        assertEquals(indexResponse.getIndex(), "testidx");
        assertEquals(indexResponse.getType(), "testtype");
        assertEquals(indexResponse.getId(), ID);
        assertEquals(indexResponse.getVersion(), 1);
        assertEquals(indexResponse.isCreated(), true);
    }

}
