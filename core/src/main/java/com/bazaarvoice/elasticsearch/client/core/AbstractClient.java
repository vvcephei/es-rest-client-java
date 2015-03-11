package com.bazaarvoice.elasticsearch.client.core;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.exists.ExistsRequestBuilder;
import org.elasticsearch.action.exists.ExistsResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.mlt.MoreLikeThisRequestBuilder;
import org.elasticsearch.action.percolate.MultiPercolateRequest;
import org.elasticsearch.action.percolate.MultiPercolateRequestBuilder;
import org.elasticsearch.action.percolate.MultiPercolateResponse;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.termvector.MultiTermVectorsRequest;
import org.elasticsearch.action.termvector.MultiTermVectorsRequestBuilder;
import org.elasticsearch.action.termvector.MultiTermVectorsResponse;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.termvector.TermVectorRequestBuilder;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * The ES client interface provides multiple options for calling each logical method.
 * This class implements all of them in terms of just one of them, leaving extenders
 * to implement only one variant.
 * <p/>
 * One thing to note is that some methods exist to support thicker es clients.
 * For example, the transport and node clients do sniffing and round-robining, etc.
 * The http client, however is a thin client for the poc, so some methods throw or return dummy values
 */
public abstract class AbstractClient implements Client {
    @Override public ActionFuture<IndexResponse> index(final IndexRequest request) {
        PlainActionFuture<IndexResponse> future = new PlainActionFuture<IndexResponse>();
        index(request, future);
        return future;
    }

    @Override public IndexRequestBuilder prepareIndex() {
        return new IndexRequestBuilder(this, null);
    }


    @Override public IndexRequestBuilder prepareIndex(final String index, final String type) {
        return new IndexRequestBuilder(this, index).setType(type);
    }

    @Override public IndexRequestBuilder prepareIndex(final String index, final String type, @Nullable final String id) {
        return new IndexRequestBuilder(this, index).setType(type).setId(id);
    }

    @Override public ActionFuture<UpdateResponse> update(final UpdateRequest request) {
        PlainActionFuture<UpdateResponse> future = new PlainActionFuture<UpdateResponse>();
        update(request, future);
        return future;
    }

    @Override public UpdateRequestBuilder prepareUpdate() {
        return new UpdateRequestBuilder(this);
    }

    @Override public UpdateRequestBuilder prepareUpdate(final String index, final String type, final String id) {
        return new UpdateRequestBuilder(this, index, type, id);
    }

    @Override public ActionFuture<DeleteResponse> delete(final DeleteRequest request) {
        PlainActionFuture<DeleteResponse> future = new PlainActionFuture<DeleteResponse>();
        delete(request, future);
        return future;
    }

    @Override public DeleteRequestBuilder prepareDelete() {
        return new DeleteRequestBuilder(this);
    }

    @Override public DeleteRequestBuilder prepareDelete(final String index, final String type, final String id) {
        return new DeleteRequestBuilder(this, index).setType(type).setId(id);
    }

    @Override public ActionFuture<BulkResponse> bulk(final BulkRequest request) {
        PlainActionFuture<BulkResponse> future = new PlainActionFuture<BulkResponse>();
        bulk(request, future);
        return future;
    }

    @Override public BulkRequestBuilder prepareBulk() {
        return new BulkRequestBuilder(this);
    }

    @Override public ActionFuture<DeleteByQueryResponse> deleteByQuery(final DeleteByQueryRequest request) {
        PlainActionFuture<DeleteByQueryResponse> future = new PlainActionFuture<DeleteByQueryResponse>();
        deleteByQuery(request, future);
        return future;
    }

    @Override public DeleteByQueryRequestBuilder prepareDeleteByQuery(final String... indices) {
        return new DeleteByQueryRequestBuilder(this).setIndices(indices);
    }

    @Override public ActionFuture<GetResponse> get(final GetRequest request) {
        PlainActionFuture<GetResponse> future = new PlainActionFuture<GetResponse>();
        get(request, future);
        return future;
    }

    @Override public GetRequestBuilder prepareGet() {
        return new GetRequestBuilder(this);
    }

    @Override public GetRequestBuilder prepareGet(final String index, @Nullable final String type, final String id) {
        return new GetRequestBuilder(this, index).setType(type).setId(id);
    }

    @Override public ActionFuture<MultiGetResponse> multiGet(final MultiGetRequest request) {
        PlainActionFuture<MultiGetResponse> future = new PlainActionFuture<MultiGetResponse>();
        multiGet(request, future);
        return future;
    }

    @Override public MultiGetRequestBuilder prepareMultiGet() {
        return new MultiGetRequestBuilder(this);
    }

    @Override public ActionFuture<CountResponse> count(final CountRequest request) {
        PlainActionFuture<CountResponse> future = new PlainActionFuture<CountResponse>();
        count(request, future);
        return future;
    }

    @Override public CountRequestBuilder prepareCount(final String... indices) {
        return new CountRequestBuilder(this).setIndices(indices);
    }

    @Override public ActionFuture<SuggestResponse> suggest(final SuggestRequest request) {
        PlainActionFuture<SuggestResponse> future = new PlainActionFuture<SuggestResponse>();
        suggest(request, future);
        return future;
    }

    @Override public SuggestRequestBuilder prepareSuggest(final String... indices) {
        return new SuggestRequestBuilder(this).setIndices(indices);
    }

    @Override public ActionFuture<SearchResponse> search(final SearchRequest request) {
        PlainActionFuture<SearchResponse> future = new PlainActionFuture<SearchResponse>();
        search(request, future);
        return future;
    }

    @Override public SearchRequestBuilder prepareSearch(final String... indices) {
        return new SearchRequestBuilder(this).setIndices(indices);
    }

    @Override public ActionFuture<SearchResponse> searchScroll(final SearchScrollRequest request) {
        PlainActionFuture<SearchResponse> future = new PlainActionFuture<SearchResponse>();
        searchScroll(request, future);
        return future;
    }

    @Override public SearchScrollRequestBuilder prepareSearchScroll(final String scrollId) {
        return new SearchScrollRequestBuilder(this, scrollId);
    }

    @Override public ActionFuture<MultiSearchResponse> multiSearch(final MultiSearchRequest request) {
        PlainActionFuture<MultiSearchResponse> future = new PlainActionFuture<MultiSearchResponse>();
        multiSearch(request, future);
        return future;
    }

    @Override public MultiSearchRequestBuilder prepareMultiSearch() {
        return new MultiSearchRequestBuilder(this);
    }

    @Override public ActionFuture<SearchResponse> moreLikeThis(final MoreLikeThisRequest request) {
        PlainActionFuture<SearchResponse> future = new PlainActionFuture<SearchResponse>();
        moreLikeThis(request, future);
        return future;
    }

    @Override public MoreLikeThisRequestBuilder prepareMoreLikeThis(final String index, final String type, final String id) {
        return new MoreLikeThisRequestBuilder(this, index, type, id);
    }

    @Override public ActionFuture<PercolateResponse> percolate(final PercolateRequest request) {
        PlainActionFuture<PercolateResponse> future = new PlainActionFuture<PercolateResponse>();
        percolate(request, future);
        return future;
    }

    @Override public PercolateRequestBuilder preparePercolate() { return new PercolateRequestBuilder(this); }

    @Override public ExplainRequestBuilder prepareExplain(final String index, final String type, final String id) {
        return new ExplainRequestBuilder(this, index, type, id);
    }

    @Override public ActionFuture<ExplainResponse> explain(final ExplainRequest request) {
        PlainActionFuture<ExplainResponse> future = new PlainActionFuture<ExplainResponse>();
        explain(request, future);
        return future;
    }

    @Override public ClearScrollRequestBuilder prepareClearScroll() {
        return new ClearScrollRequestBuilder(this);
    }

    @Override public ActionFuture<ClearScrollResponse> clearScroll(final ClearScrollRequest request) {
        PlainActionFuture<ClearScrollResponse> future = new PlainActionFuture<ClearScrollResponse>();
        clearScroll(request, future);
        return future;
    }

    @Override public ActionFuture<PutIndexedScriptResponse> putIndexedScript(final PutIndexedScriptRequest request) {
        PlainActionFuture<PutIndexedScriptResponse> future = new PlainActionFuture<PutIndexedScriptResponse>();
        putIndexedScript(request, future);
        return future;
    }

    @Override public PutIndexedScriptRequestBuilder preparePutIndexedScript() { return new PutIndexedScriptRequestBuilder(this); }

    @Override
    public PutIndexedScriptRequestBuilder preparePutIndexedScript(final String scriptLang, final String id, final String source) { return new PutIndexedScriptRequestBuilder(this).setScriptLang(scriptLang).setId(id).setSource(source); }

    @Override public ActionFuture<DeleteIndexedScriptResponse> deleteIndexedScript(final DeleteIndexedScriptRequest request) {
        final PlainActionFuture<DeleteIndexedScriptResponse> future = new PlainActionFuture<DeleteIndexedScriptResponse>();
        deleteIndexedScript(request, future);
        return future;
    }

    @Override public DeleteIndexedScriptRequestBuilder prepareDeleteIndexedScript() { return new DeleteIndexedScriptRequestBuilder(this); }

    @Override
    public DeleteIndexedScriptRequestBuilder prepareDeleteIndexedScript(final String scriptLang, final String id) { return new DeleteIndexedScriptRequestBuilder(this).setScriptLang(scriptLang).setId(id); }

    @Override public ActionFuture<GetIndexedScriptResponse> getIndexedScript(final GetIndexedScriptRequest request) {
        final PlainActionFuture<GetIndexedScriptResponse> future = new PlainActionFuture<GetIndexedScriptResponse>();
        getIndexedScript(request, future);
        return future;
    }

    @Override public GetIndexedScriptRequestBuilder prepareGetIndexedScript() { return new GetIndexedScriptRequestBuilder(this); }

    @Override
    public GetIndexedScriptRequestBuilder prepareGetIndexedScript(final String scriptLang, final String id) { return new GetIndexedScriptRequestBuilder(this).setScriptLang(scriptLang).setId(id); }


    @Override public ActionFuture<ExistsResponse> exists(final ExistsRequest request) {
        final PlainActionFuture<ExistsResponse> future = new PlainActionFuture<ExistsResponse>();
        exists(request, future);
        return future;
    }

    @Override public ExistsRequestBuilder prepareExists(final String... indices) { return new ExistsRequestBuilder(this).setIndices(indices); }

    @Override public ActionFuture<TermVectorResponse> termVector(final TermVectorRequest request) {
        final PlainActionFuture<TermVectorResponse> future = new PlainActionFuture<TermVectorResponse>();
        termVector(request, future);
        return future;
    }

    @Override public TermVectorRequestBuilder prepareTermVector() { return new TermVectorRequestBuilder(this); }

    @Override public TermVectorRequestBuilder prepareTermVector(final String index, final String type, final String id) { return new TermVectorRequestBuilder(this, index, type, id); }

    @Override public ActionFuture<MultiTermVectorsResponse> multiTermVectors(final MultiTermVectorsRequest request) {
        final PlainActionFuture<MultiTermVectorsResponse> future = new PlainActionFuture<MultiTermVectorsResponse>();
        multiTermVectors(request, future);
        return future;
    }

    @Override public MultiTermVectorsRequestBuilder prepareMultiTermVectors() { return new MultiTermVectorsRequestBuilder(this); }

    @Override public ActionFuture<MultiPercolateResponse> multiPercolate(final MultiPercolateRequest request) {
        final PlainActionFuture<MultiPercolateResponse> future = new PlainActionFuture<MultiPercolateResponse>();
        multiPercolate(request, future);
        return future;
    }

    @Override public MultiPercolateRequestBuilder prepareMultiPercolate() { return new MultiPercolateRequestBuilder(this); }


    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> RequestBuilder prepareExecute(final Action<Request, Response, RequestBuilder, Client> action) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> void execute(final Action<Request, Response, RequestBuilder, Client> action, final Request request, final ActionListener<Response> listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>> ActionFuture<Response> execute(final Action<Request, Response, RequestBuilder, Client> action, final Request request) {
        throw new UnsupportedOperationException();
    }

    @Override public Settings settings() { throw new UnsupportedOperationException(); }

    @Override public ThreadPool threadPool() {
        final ThreadPool dummyThreadPool = new ThreadPool(ImmutableSettings.builder().build(), null);
//        throw new UnsupportedOperationException();
        return dummyThreadPool;
    }

}
