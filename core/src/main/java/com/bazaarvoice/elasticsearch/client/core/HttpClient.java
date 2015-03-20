package com.bazaarvoice.elasticsearch.client.core;

import com.bazaarvoice.elasticsearch.client.core.spi.HttpExecutor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.XContentResponseTransform;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.DeleteResponseHelper;
import org.elasticsearch.action.delete.DeleteRest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.exists.ExistsResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.GetResponseHelper;
import org.elasticsearch.action.get.GetRest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.IndexResponseHelper;
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
import org.elasticsearch.action.search.SearchResponseHelper;
import org.elasticsearch.action.search.SearchRest;
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

import static org.elasticsearch.action.NotifyingCallback.callback;

/**
 * An implementation of org.elasticsearch.client.Client that uses
 * the REST+JSON ES api rather than the binary ES transport.
 * <p/>
 * This client does NOT have an opinion about how requests will be
 * sent over the wire. It delegates that responsibility to whatever
 * implementation of {@link com.bazaarvoice.elasticsearch.client.core.spi.HttpExecutor}
 * you choose to supply.
 */
public class HttpClient extends AbstractClient implements Client {

    private final IndexRest<IndexResponse> indexRest;
    private final GetRest<GetResponse> getRest;
    private final DeleteRest<DeleteResponse> deleteRest;
    private final SearchRest<SearchResponse> searchRest;

    public static HttpClient withExecutor(final String protocol, final String host, final int port, final HttpExecutor executor) {
        return new HttpClient(protocol, host, port, executor);
    }

    private HttpClient(final String protocol, final String host, final int port, final HttpExecutor executor) {
        indexRest = new IndexRest<IndexResponse>(protocol, host, port, executor, new XContentResponseTransform<IndexResponse>(new IndexResponseHelper()));
        getRest = new GetRest<GetResponse>(protocol, host, port, executor, new XContentResponseTransform<GetResponse>(new GetResponseHelper()));
        deleteRest = new DeleteRest<DeleteResponse>(protocol, host, port, executor, new XContentResponseTransform<DeleteResponse>(new DeleteResponseHelper()));
        searchRest = new SearchRest<SearchResponse>(protocol, host, port, executor, new XContentResponseTransform<SearchResponse>(new SearchResponseHelper()));
    }

    @Override public void close() {
        // nothing to close at the moment
    }

    @Override public AdminClient admin() {
        // TODO admin
        return null;
    }

    @Override public void get(final GetRequest request, final ActionListener<GetResponse> listener) {
        Futures.addCallback(getRest.act(request), callback(listener));
    }

    @Override public void index(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        Futures.addCallback(indexRest.act(request), callback(listener));
    }

    @Override public void delete(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        Futures.addCallback(deleteRest.act(request), callback(listener));
    }

    @Override public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
        Futures.addCallback(searchRest.act(request), callback(listener));
    }


    @Override public void update(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {

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
