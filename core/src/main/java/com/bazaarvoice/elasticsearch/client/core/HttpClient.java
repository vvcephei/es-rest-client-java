package com.bazaarvoice.elasticsearch.client.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.exists.ExistsResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.GetRest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.IndexRest;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.percolate.MultiPercolateRequest;
import org.elasticsearch.action.percolate.MultiPercolateResponse;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.termvector.MultiTermVectorsRequest;
import org.elasticsearch.action.termvector.MultiTermVectorsResponse;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.Futures;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

public class HttpClient extends AbstractClient implements Client {

    private final IndexRest indexRest;
    private final GetRest getRest;

    public static HttpClient withExecutor(String protocol, String host, int port, HttpExecutor executor) {
        return new HttpClient(protocol, host, port, executor);
    }

    private HttpClient(String protocol, String host, int port, HttpExecutor executor) {
        indexRest = new IndexRest(protocol, host, port, executor);
        getRest = new GetRest(protocol, host, port, executor);
    }

    @Override public void close() {
        // TODO: consider whether we need to close the executor or just let the provider manage its lifecycle independently
    }

    @Override public AdminClient admin() {
        // TODO admin
        return null;
    }

    @Override public void get(final GetRequest request, final ActionListener<GetResponse> listener) {
        Futures.addCallback(getRest.act(request), GetRest.callback(listener));
    }

    @Override public void index(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        Futures.addCallback(indexRest.act(request), IndexRest.indexResponseCallback(listener));
    }

    @Override public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {

    }


    @Override public void update(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {

    }

    @Override public void delete(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {

    }

    @Override public void bulk(final BulkRequest request, final ActionListener<BulkResponse> listener) {

    }

    @Override public void deleteByQuery(final DeleteByQueryRequest request, final ActionListener<DeleteByQueryResponse> listener) {

    }


    @Override public void multiGet(final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {

    }

    @Override public void count(final CountRequest request, final ActionListener<CountResponse> listener) {

    }

    @Override public void suggest(final SuggestRequest request, final ActionListener<SuggestResponse> listener) {

    }


    @Override public void searchScroll(final SearchScrollRequest request, final ActionListener<SearchResponse> listener) {

    }

    @Override public void multiSearch(final MultiSearchRequest request, final ActionListener<MultiSearchResponse> listener) {

    }

    @Override public void moreLikeThis(final MoreLikeThisRequest request, final ActionListener<SearchResponse> listener) {

    }

    @Override public void percolate(final PercolateRequest request, final ActionListener<PercolateResponse> listener) {

    }


    @Override public void explain(final ExplainRequest request, final ActionListener<ExplainResponse> listener) {

    }

    @Override public void clearScroll(final ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {

    }

    @Override public void putIndexedScript(final PutIndexedScriptRequest request, final ActionListener<PutIndexedScriptResponse> listener) { }

    @Override public void deleteIndexedScript(final DeleteIndexedScriptRequest request, final ActionListener<DeleteIndexedScriptResponse> listener) { }

    @Override public void getIndexedScript(final GetIndexedScriptRequest request, final ActionListener<GetIndexedScriptResponse> listener) { }

    @Override public void exists(final ExistsRequest request, final ActionListener<ExistsResponse> listener) { }

    @Override public void termVector(final TermVectorRequest request, final ActionListener<TermVectorResponse> listener) { }

    @Override public void multiTermVectors(final MultiTermVectorsRequest request, final ActionListener<MultiTermVectorsResponse> listener) { }

    @Override public void multiPercolate(final MultiPercolateRequest request, final ActionListener<MultiPercolateResponse> listener) { }


}
